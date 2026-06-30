# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 — Silver (Clean / Standardize / Dedupe)
# MAGIC
# MAGIC **Payment Settlement & Reconciliation Lakehouse — Medallion Architecture**
# MAGIC
# MAGIC This notebook transforms the **Bronze** raw-ingest tables into curated **Silver** tables.
# MAGIC
# MAGIC ### What this phase does
# MAGIC 1. **Reads** `bronze_internal` and `bronze_network` (produced by Phase 2 Auto Loader ingest).
# MAGIC 2. **Cleans & standardizes**: trim + upper-case `channel` and `status`, cast `amount` to
# MAGIC    `decimal(18,2)`, cast `business_date` to `date`, `txn_ts` to `timestamp`.
# MAGIC 3. **Data quality**: rows with `null txn_id` or `amount <= 0` are *rejected* (not dropped silently)
# MAGIC    and written with a `reject_reason` column to `silver_internal_rejects` / `silver_network_rejects`.
# MAGIC 4. **Deduplicates** by `txn_id`, keeping the latest record by `txn_ts` then `_ingest_ts`.
# MAGIC 5. **Writes** `silver_internal` and `silver_network` as Delta (overwrite, idempotent).
# MAGIC 6. **Verifies** with clean-vs-rejected counts per side.
# MAGIC
# MAGIC Runnable top-to-bottom on Databricks **Free Edition** (serverless). Idempotent.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — shared constants
# MAGIC These are repeated verbatim in every phase notebook so the table/column contract lines up.

# COMMAND ----------

# Shared constants (identical across all phase notebooks) -------------------------------------------
CATALOG = "workspace"            # Free Edition default; if permission error, swap to an existing catalog
SCHEMA  = "settlement_recon"
VOLUME  = "landing"

VOLUME_ROOT   = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
INTERNAL_PATH = f"{VOLUME_ROOT}/internal"
NETWORK_PATH  = f"{VOLUME_ROOT}/network"

# Fully-qualified table names (naming contract) ----------------------------------------------------
BRONZE_INTERNAL = f"{CATALOG}.{SCHEMA}.bronze_internal"
BRONZE_NETWORK  = f"{CATALOG}.{SCHEMA}.bronze_network"

SILVER_INTERNAL         = f"{CATALOG}.{SCHEMA}.silver_internal"
SILVER_NETWORK          = f"{CATALOG}.{SCHEMA}.silver_network"
SILVER_INTERNAL_REJECTS = f"{CATALOG}.{SCHEMA}.silver_internal_rejects"
SILVER_NETWORK_REJECTS  = f"{CATALOG}.{SCHEMA}.silver_network_rejects"

# Make the active catalog/schema explicit so unqualified references resolve correctly.
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print("Bronze inputs :", BRONZE_INTERNAL, "|", BRONZE_NETWORK)
print("Silver outputs:", SILVER_INTERNAL, "|", SILVER_NETWORK)
print("Reject tables :", SILVER_INTERNAL_REJECTS, "|", SILVER_NETWORK_REJECTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports & reusable cleaning logic
# MAGIC The `internal` and `network` schemas are identical except `network` carries an extra
# MAGIC `network_ref` column, so we drive both sides through one parameterized helper.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

# Canonical column order for the cleaned Silver output (network adds network_ref) ------------------
INTERNAL_COLS = [
    "txn_id", "business_date", "channel", "amount", "currency",
    "status", "account_id", "txn_ts", "_ingest_ts", "_source_file",
]
NETWORK_EXTRA_COLS = ["network_ref"]


def standardize(df: DataFrame, is_network: bool) -> DataFrame:
    """Trim/upper string codes and cast columns to their canonical types.

    Casting is done defensively because Bronze landed everything via Auto Loader from CSV; even
    where Bronze already typed a column, re-casting here is a no-op and keeps Silver self-contained.
    """
    out = (
        df
        # Standardize categorical codes: trim whitespace then upper-case.
        .withColumn("channel", F.upper(F.trim(F.col("channel"))))
        .withColumn("status",  F.upper(F.trim(F.col("status"))))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn("txn_id",     F.trim(F.col("txn_id")))
        .withColumn("account_id", F.trim(F.col("account_id")))
        # Type casting per contract.
        .withColumn("amount",        F.col("amount").cast("decimal(18,2)"))
        .withColumn("business_date", F.col("business_date").cast("date"))
        .withColumn("txn_ts",        F.col("txn_ts").cast("timestamp"))
        .withColumn("_ingest_ts",    F.col("_ingest_ts").cast("timestamp"))
    )
    if is_network:
        out = out.withColumn("network_ref", F.trim(F.col("network_ref")))
    return out


def split_quality(df: DataFrame):
    """Split a standardized DataFrame into (clean, rejects).

    Reject rules (per spec): null/empty txn_id, or amount is null / <= 0.
    Rejects carry a human-readable `reject_reason`. The original (pre-clean) columns are preserved
    so the reject tables remain useful for triage.
    """
    txn_bad    = F.col("txn_id").isNull() | (F.length(F.col("txn_id")) == 0)
    amount_bad = F.col("amount").isNull() | (F.col("amount") <= 0)

    reject_reason = F.concat_ws(
        "; ",
        F.when(txn_bad,    F.lit("null_or_empty_txn_id")),
        F.when(amount_bad, F.lit("amount_le_zero_or_null")),
    )

    is_reject = txn_bad | amount_bad
    rejects = df.filter(is_reject).withColumn("reject_reason", reject_reason)
    clean   = df.filter(~is_reject)
    return clean, rejects


def dedupe_latest(df: DataFrame) -> DataFrame:
    """Keep one row per txn_id: the latest by txn_ts, tie-broken by _ingest_ts (both desc).

    Nulls sort last so a row with a real timestamp always wins over one without.
    """
    w = (
        Window.partitionBy("txn_id")
        .orderBy(F.col("txn_ts").desc_nulls_last(), F.col("_ingest_ts").desc_nulls_last())
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def build_silver(bronze_table: str, is_network: bool):
    """Full Bronze->Silver pipeline for one side. Returns (clean_deduped, rejects)."""
    bronze = spark.read.table(bronze_table)
    std = standardize(bronze, is_network)
    clean, rejects = split_quality(std)
    deduped = dedupe_latest(clean)

    # Project to the canonical column order (only columns that actually exist).
    cols = INTERNAL_COLS + (NETWORK_EXTRA_COLS if is_network else [])
    cols = [c for c in cols if c in deduped.columns]
    deduped = deduped.select(*cols)
    return deduped, rejects

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Silver — internal side
# MAGIC Clean -> quality-split -> dedupe by `txn_id` (latest `txn_ts`, then `_ingest_ts`).

# COMMAND ----------

internal_clean, internal_rejects = build_silver(BRONZE_INTERNAL, is_network=False)

(
    internal_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_INTERNAL)
)

(
    internal_rejects.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_INTERNAL_REJECTS)
)

print("Wrote", SILVER_INTERNAL, "and", SILVER_INTERNAL_REJECTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Silver — network side
# MAGIC Same pipeline; carries the extra `network_ref` column through to Silver.

# COMMAND ----------

network_clean, network_rejects = build_silver(BRONZE_NETWORK, is_network=True)

(
    network_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_NETWORK)
)

(
    network_rejects.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_NETWORK_REJECTS)
)

print("Wrote", SILVER_NETWORK, "and", SILVER_NETWORK_REJECTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification — clean vs rejected counts
# MAGIC Sanity-checks the run: Bronze input rows, clean (post-dedupe) rows, rejected rows, and the
# MAGIC number of duplicate `txn_id`s collapsed by deduplication.

# COMMAND ----------

def report(side: str, bronze_table: str, silver_table: str, reject_table: str):
    bronze_cnt  = spark.read.table(bronze_table).count()
    silver_cnt  = spark.read.table(silver_table).count()
    reject_cnt  = spark.read.table(reject_table).count()
    # Duplicates collapsed = clean-before-dedupe minus distinct kept rows.
    # clean-before-dedupe = bronze - rejects (rejects were removed before dedupe).
    dupes_collapsed = (bronze_cnt - reject_cnt) - silver_cnt
    return (side, bronze_cnt, silver_cnt, reject_cnt, dupes_collapsed)

rows = [
    report("internal", BRONZE_INTERNAL, SILVER_INTERNAL, SILVER_INTERNAL_REJECTS),
    report("network",  BRONZE_NETWORK,  SILVER_NETWORK,  SILVER_NETWORK_REJECTS),
]

summary = spark.createDataFrame(
    rows,
    schema="side string, bronze_rows long, silver_clean_rows long, rejected_rows long, dupes_collapsed long",
)
print("=== Phase 3 Silver verification ===")
summary.show(truncate=False)

# Breakdown of reject reasons (helpful for triage). ------------------------------------------------
print("Internal reject reasons:")
spark.read.table(SILVER_INTERNAL_REJECTS).groupBy("reject_reason").count().show(truncate=False)
print("Network reject reasons:")
spark.read.table(SILVER_NETWORK_REJECTS).groupBy("reject_reason").count().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done-criteria & hand-off
# MAGIC
# MAGIC **This phase is complete when:**
# MAGIC - `silver_internal` and `silver_network` exist as Delta tables, cleaned/standardized/typed and
# MAGIC   **deduplicated to one row per `txn_id`** (latest by `txn_ts`, then `_ingest_ts`).
# MAGIC - `silver_internal_rejects` and `silver_network_rejects` capture all dropped rows with a
# MAGIC   `reject_reason` (null/empty `txn_id` or `amount <= 0`).
# MAGIC - The verification cell shows `silver_clean + rejected + dupes_collapsed == bronze` for each side.
# MAGIC
# MAGIC **Silver output schema (per side):**
# MAGIC `txn_id, business_date(date), channel, amount(decimal(18,2)), currency, status, account_id,`
# MAGIC `txn_ts(timestamp), _ingest_ts(timestamp), _source_file` — plus `network_ref` on the network side.
# MAGIC
# MAGIC **Next phase consumes this:**
# MAGIC - **Phase 4 (Gold)** reads `silver_internal` + `silver_network`, joins on `txn_id` to produce
# MAGIC   `gold_recon_results` (matched / amount-mismatch / status-mismatch / missing-in-network) and
# MAGIC   `gold_exception_cases`. Clean, deduped, type-aligned Silver is the precondition for that join.
