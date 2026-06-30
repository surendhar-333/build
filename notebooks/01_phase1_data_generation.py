# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 — Synthetic Data Generation & Landing
# MAGIC
# MAGIC **Project:** Payment Settlement & Reconciliation Lakehouse
# MAGIC
# MAGIC This notebook generates two sides of a payment dataset and lands them as files in a Unity Catalog volume:
# MAGIC - **internal** — our source-of-truth debit records (the system of record)
# MAGIC - **network** — the bank/network side, derived from internal with deliberate discrepancies injected
# MAGIC   (missing records, amount mismatches, status mismatches) so the reconciliation engine has something to find.
# MAGIC
# MAGIC Built on plain PySpark `spark.range()` so it scales from 100K to 10M+ rows with zero external dependencies.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Config (catalog / schema / volume)
# MAGIC
# MAGIC On Free Edition the default catalog is usually `workspace`. If `CREATE CATALOG` fails with a
# MAGIC permissions error, set `CATALOG` to a catalog you can already see in the Catalog browser and re-run.

# COMMAND ----------

# ---- Configuration ---------------------------------------------------------
CATALOG = "workspace"            # Free Edition default; change if you lack CREATE CATALOG rights
SCHEMA = "settlement_recon"
VOLUME = "landing"

# Volume landing paths (Bronze will read from these in Phase 2)
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
INTERNAL_PATH = f"{VOLUME_ROOT}/internal"
NETWORK_PATH = f"{VOLUME_ROOT}/network"

# Generation parameters
# Phase 6's scale lab drives row count via a "rows" widget passed through dbutils.notebook.run.
# When run standalone the widget defaults to 100_000; bump it (or pass rows=1000000) for the scale lab.
dbutils.widgets.text("rows", "100000")
ROWS_INTERNAL = int(dbutils.widgets.get("rows"))   # default 100_000; overridden by Phase 6 scale lab
NETWORK_DROP_RATE = 0.05         # ~5% of network records go missing  -> UNMATCHED exceptions
AMOUNT_MISMATCH_RATE = 0.02      # ~2% have a wrong amount             -> MISMATCH exceptions
STATUS_MISMATCH_RATE = 0.02      # ~2% have a wrong status             -> MISMATCH exceptions
BUSINESS_DATE = "2026-06-30"     # the settlement day we are generating

CHANNELS = ["ATM", "POS", "ECOM", "WALLET", "IMPS"]
STATUSES = ["SETTLED", "PENDING", "FAILED", "REVERSED"]

# Create catalog / schema / volume (idempotent)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

print("Config ready.")
print("  Internal landing:", INTERNAL_PATH)
print("  Network  landing:", NETWORK_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Generator functions
# MAGIC
# MAGIC `build_internal()` creates the source of truth. `build_network()` derives the network side from it,
# MAGIC dropping a fraction of rows and corrupting amount/status on a fraction — these are the discrepancies
# MAGIC the reconciliation engine (Gold) will detect and classify into exception cases.

# COMMAND ----------

from pyspark.sql import functions as F, DataFrame


def build_internal(n_rows: int, business_date: str) -> DataFrame:
    """Source-of-truth internal debit records, deterministic and scalable via spark.range()."""
    df = spark.range(0, n_rows).withColumnRenamed("id", "seq")

    # Stable, real-looking IDs derived from the sequence number
    txn_id = F.concat(F.lit("TXN"), F.lpad(F.col("seq").cast("string"), 12, "0"))

    # Spread values deterministically using the sequence + a hash for variety
    h = F.abs(F.hash(F.col("seq")))

    return (
        df.select(
            txn_id.alias("txn_id"),
            F.lit(business_date).cast("date").alias("business_date"),
            F.element_at(F.array(*[F.lit(c) for c in CHANNELS]), (F.col("seq") % len(CHANNELS) + 1).cast("int")).alias("channel"),
            # amount between 10.00 and 5000.00, two decimals
            (F.round((h % 499000) / 100.0 + 10.0, 2)).alias("amount"),
            F.lit("INR").alias("currency"),
            F.element_at(F.array(*[F.lit(s) for s in STATUSES]), (h % len(STATUSES) + 1).cast("int")).alias("status"),
            F.concat(F.lit("ACCT"), F.lpad((h % 50000).cast("string"), 8, "0")).alias("account_id"),
            # business_date is date-only; give unix_timestamp the matching format so ANSI mode parses it,
            # then spread txn_ts across the day by adding seconds derived from the row sequence.
            (F.unix_timestamp(F.lit(business_date), "yyyy-MM-dd") + (F.col("seq") % 86400)).cast("timestamp").alias("txn_ts"),
        )
    )


def build_network(internal_df: DataFrame) -> DataFrame:
    """Network/bank side derived from internal, with injected discrepancies.

    - drop ~NETWORK_DROP_RATE of rows  -> they will be UNMATCHED at recon time
    - corrupt amount on ~AMOUNT_MISMATCH_RATE -> MISMATCH
    - corrupt status on ~STATUS_MISMATCH_RATE -> MISMATCH
    Uses a deterministic per-row uniform in [0,1) from the hash so results are reproducible.
    """
    r = (F.abs(F.hash(F.col("txn_id"), F.lit(7))) % 100000) / 100000.0  # uniform-ish [0,1)

    corrupted = (
        internal_df
        # drop a fraction of records (simulates network never reporting them)
        .where(r >= F.lit(NETWORK_DROP_RATE))
        # amount mismatch: bump amount by 10% for a fraction
        .withColumn(
            "amount",
            F.when(
                (r >= F.lit(NETWORK_DROP_RATE)) & (r < F.lit(NETWORK_DROP_RATE + AMOUNT_MISMATCH_RATE)),
                F.round(F.col("amount") * 1.10, 2),
            ).otherwise(F.col("amount")),
        )
        # status mismatch: flip status to SETTLED for a different fraction
        .withColumn(
            "status",
            F.when(
                (r >= F.lit(NETWORK_DROP_RATE + AMOUNT_MISMATCH_RATE))
                & (r < F.lit(NETWORK_DROP_RATE + AMOUNT_MISMATCH_RATE + STATUS_MISMATCH_RATE)),
                F.lit("SETTLED"),
            ).otherwise(F.col("status")),
        )
        # network uses its own settlement reference id
        .withColumn("network_ref", F.concat(F.lit("NET"), F.substring(F.col("txn_id"), 4, 12)))
    )
    return corrupted


print("Generator functions defined: build_internal(), build_network()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Generate one business day and land the files
# MAGIC
# MAGIC Writes CSV (header, single coalesced part for easy inspection) to the internal/ and network/ folders.
# MAGIC Phase 2 (Bronze) points Auto Loader at these folders.

# COMMAND ----------

internal_df = build_internal(ROWS_INTERNAL, BUSINESS_DATE)
network_df = build_network(internal_df)

# Land as CSV partitioned by business_date subfolder so daily runs accumulate
(
    internal_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{INTERNAL_PATH}/business_date={BUSINESS_DATE}")
)
(
    network_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{NETWORK_PATH}/business_date={BUSINESS_DATE}")
)

internal_count = internal_df.count()
network_count = network_df.count()

print(f"Internal rows landed: {internal_count:,}")
print(f"Network  rows landed: {network_count:,}  (expect ~{int(ROWS_INTERNAL * (1 - NETWORK_DROP_RATE)):,})")
print(f"Dropped (will be UNMATCHED): ~{internal_count - network_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check — eyeball both sides

# COMMAND ----------

print("=== INTERNAL (source of truth) ===")
internal_df.show(5, truncate=False)
print("=== NETWORK (with injected discrepancies) ===")
network_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Phase 1 done when:** internal ≈ 100,000, network slightly fewer, and the samples look like real
# MAGIC transactions. Next: **Phase 2 — Bronze with Auto Loader** (`cloudFiles` reads internal/ and network/,
# MAGIC streams into `bronze_internal` / `bronze_network` Delta tables with checkpointing).
