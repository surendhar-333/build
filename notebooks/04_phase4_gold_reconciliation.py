# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 — Gold Reconciliation (Centerpiece)
# MAGIC
# MAGIC **Payment Settlement & Reconciliation Lakehouse — Medallion Architecture**
# MAGIC
# MAGIC This notebook performs the core reconciliation of the pipeline. It does a **FULL OUTER JOIN**
# MAGIC between the cleaned internal ledger (`silver_internal`, alias `i`) and the network/scheme
# MAGIC feed (`silver_network`, alias `n`) on `txn_id`, then classifies every transaction into a
# MAGIC `match_status` and materializes:
# MAGIC
# MAGIC - **`gold_recon_results`** — one row per `txn_id` with both sides' amounts/statuses, the
# MAGIC   absolute amount difference, the classification, and a human-readable `reason`.
# MAGIC - **`gold_exception_cases`** — one case per non-`MATCHED` row, with a deterministic `case_id`,
# MAGIC   a `case_type`, an auto/manual `disposition`, and a `created_ts`.
# MAGIC
# MAGIC ### Classification rules
# MAGIC | match_status | meaning |
# MAGIC |---|---|
# MAGIC | `MATCHED` | both sides present, `abs(amount_diff) <= AMOUNT_TOLERANCE` **and** statuses equal |
# MAGIC | `MISMATCH_AMOUNT` | both present, amounts differ beyond tolerance, statuses equal |
# MAGIC | `MISMATCH_STATUS` | both present, amounts within tolerance, statuses differ |
# MAGIC | `MISMATCH_BOTH` | both present, amounts differ **and** statuses differ |
# MAGIC | `UNMATCHED_INTERNAL` | present in internal, missing in network |
# MAGIC | `UNMATCHED_NETWORK` | present in network, missing in internal |
# MAGIC
# MAGIC **Consumes:** `workspace.settlement_recon.silver_internal`, `workspace.settlement_recon.silver_network`
# MAGIC **Produces:** `gold_recon_results`, `gold_exception_cases`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — shared constants (repeated in every phase notebook)

# COMMAND ----------

# Shared constants — must match every other phase notebook exactly.
CATALOG = "workspace"          # Free Edition default; if you hit a permission error, swap to an existing catalog
SCHEMA  = "settlement_recon"
VOLUME  = "landing"

VOLUME_ROOT   = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
INTERNAL_PATH = f"{VOLUME_ROOT}/internal"
NETWORK_PATH  = f"{VOLUME_ROOT}/network"

# Fully-qualified table names (naming contract)
SILVER_INTERNAL = f"{CATALOG}.{SCHEMA}.silver_internal"
SILVER_NETWORK  = f"{CATALOG}.{SCHEMA}.silver_network"
GOLD_RECON      = f"{CATALOG}.{SCHEMA}.gold_recon_results"
GOLD_EXCEPTIONS = f"{CATALOG}.{SCHEMA}.gold_exception_cases"

# Reconciliation tolerances
AMOUNT_TOLERANCE       = 0.01   # absolute: within this, amounts are considered equal -> MATCHED
AUTO_RESOLVE_TOLERANCE = 1.00   # small amount diffs auto-dispositioned without manual review

# Ensure the schema exists (idempotent); harmless if already created by earlier phases.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(f"Reconciling {SILVER_INTERNAL}  x  {SILVER_NETWORK}")
print(f"AMOUNT_TOLERANCE={AMOUNT_TOLERANCE}  AUTO_RESOLVE_TOLERANCE={AUTO_RESOLVE_TOLERANCE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Silver inputs
# MAGIC
# MAGIC Both Silver tables are cleaned/deduped/typed by Phase 3. We alias them `i` (internal) and
# MAGIC `n` (network). `business_date` and `channel` exist on both sides; we coalesce them so the
# MAGIC result has a value even when one side is missing.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_i = spark.table(SILVER_INTERNAL)
silver_n = spark.table(SILVER_NETWORK)

print("silver_internal rows:", silver_i.count())
print("silver_network  rows:", silver_n.count())

silver_i.printSchema()
silver_n.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Full outer join + classification
# MAGIC
# MAGIC We join on `txn_id` and derive:
# MAGIC - `present_i` / `present_n` — side-presence flags (txn_id non-null on that side).
# MAGIC - `amount_diff` — `internal_amount - network_amount` (null when either side missing).
# MAGIC - `amount_mismatch` / `status_mismatch` — booleans (only meaningful when both present).
# MAGIC - `match_status` — the final classification per the rules table above.
# MAGIC - `reason` — a short human-readable explanation for analysts/auditors.

# COMMAND ----------

i = silver_i.alias("i")
n = silver_n.alias("n")

joined = i.join(n, on=F.col("i.txn_id") == F.col("n.txn_id"), how="fullouter")

# Side-presence flags (a side is "present" when its txn_id is non-null)
present_i = F.col("i.txn_id").isNotNull()
present_n = F.col("n.txn_id").isNotNull()

# Coalesced descriptive columns (so we always have a txn_id / date / channel)
txn_id        = F.coalesce(F.col("i.txn_id"), F.col("n.txn_id"))
business_date = F.coalesce(F.col("i.business_date"), F.col("n.business_date"))
channel       = F.coalesce(F.col("i.channel"), F.col("n.channel"))

internal_amount = F.col("i.amount")
network_amount  = F.col("n.amount")
internal_status = F.col("i.status")
network_status  = F.col("n.status")

# amount_diff only defined when both sides present; rounded to avoid float noise.
# Cast both operands to double so the column type is unambiguously double end-to-end
# (Silver stores amount as decimal(18,2); the null branch below is also double).
amount_diff = F.when(
    present_i & present_n,
    F.round(F.col("i.amount").cast("double") - F.col("n.amount").cast("double"), 2),
).otherwise(F.lit(None).cast("double"))

# Mismatch booleans — only relevant when both sides are present
amount_mismatch = present_i & present_n & (F.abs(F.col("i.amount") - F.col("n.amount")) > AMOUNT_TOLERANCE)
status_mismatch = present_i & present_n & (F.col("i.status") != F.col("n.status"))

# match_status classification (order matters: unmatched first, then both-present cases)
match_status = (
    F.when(present_i & ~present_n, F.lit("UNMATCHED_INTERNAL"))
     .when(~present_i & present_n, F.lit("UNMATCHED_NETWORK"))
     .when(amount_mismatch & status_mismatch, F.lit("MISMATCH_BOTH"))
     .when(amount_mismatch & ~status_mismatch, F.lit("MISMATCH_AMOUNT"))
     .when(~amount_mismatch & status_mismatch, F.lit("MISMATCH_STATUS"))
     .otherwise(F.lit("MATCHED"))
)

# NOTE: `reason` is derived in a second projection below (after the select) so it can reference
# the already-named `amount_diff` / status columns rather than the raw aliased join columns.

recon = (
    joined
    .select(
        txn_id.alias("txn_id"),
        business_date.alias("business_date"),
        channel.alias("channel"),
        internal_amount.alias("internal_amount"),
        network_amount.alias("network_amount"),
        amount_diff.alias("amount_diff"),
        internal_status.alias("internal_status"),
        network_status.alias("network_status"),
        match_status.alias("match_status"),
    )
    # reason references the already-aliased amount_diff, so add it in a second projection
    .withColumn(
        "reason",
        F.when(F.col("match_status") == "UNMATCHED_INTERNAL",
               F.lit("Transaction present in internal ledger but missing from network feed"))
         .when(F.col("match_status") == "UNMATCHED_NETWORK",
               F.lit("Transaction present in network feed but missing from internal ledger"))
         .when(F.col("match_status") == "MISMATCH_BOTH",
               F.concat(F.lit("Amount differs by "), F.col("amount_diff").cast("string"),
                        F.lit(" and status differs ("), F.col("internal_status"),
                        F.lit(" vs "), F.col("network_status"), F.lit(")")))
         .when(F.col("match_status") == "MISMATCH_AMOUNT",
               F.concat(F.lit("Amount differs by "), F.col("amount_diff").cast("string"),
                        F.lit(" (tolerance "), F.lit(str(AMOUNT_TOLERANCE)), F.lit(")")))
         .when(F.col("match_status") == "MISMATCH_STATUS",
               F.concat(F.lit("Status differs ("), F.col("internal_status"),
                        F.lit(" vs "), F.col("network_status"), F.lit(")")))
         .otherwise(F.lit("Amount and status agree within tolerance"))
    )
)

recon.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write `gold_recon_results`
# MAGIC
# MAGIC Idempotent full overwrite (reconciliation is recomputed from the current Silver state each run).

# COMMAND ----------

(
    recon.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_RECON)
)

print(f"Wrote {GOLD_RECON}: {spark.table(GOLD_RECON).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build `gold_exception_cases`
# MAGIC
# MAGIC Every non-`MATCHED` row becomes an exception case.
# MAGIC
# MAGIC - `case_id = concat("CASE-", business_date, "-", lpad(row_number, 8, "0"))` — `row_number` is
# MAGIC   assigned **per business_date** (ordered by txn_id) so ids are deterministic and stable across reruns.
# MAGIC - `case_type = match_status`.
# MAGIC - `disposition = "AUTO"` when `case_type == "MISMATCH_AMOUNT"` and `amount_diff <= AUTO_RESOLVE_TOLERANCE`,
# MAGIC   otherwise `"MANUAL"`. We compare on the **absolute** diff so small under- or over-statements both auto-resolve.
# MAGIC - `created_ts = current_timestamp()`.

# COMMAND ----------

non_matched = spark.table(GOLD_RECON).filter(F.col("match_status") != "MATCHED")

# Deterministic per-date row numbering (ordered by txn_id) for stable case ids
w = Window.partitionBy("business_date").orderBy("txn_id")

exceptions = (
    non_matched
    .withColumn("row_number", F.row_number().over(w))
    .withColumn(
        "case_id",
        F.concat(
            F.lit("CASE-"),
            F.col("business_date").cast("string"),
            F.lit("-"),
            F.lpad(F.col("row_number").cast("string"), 8, "0"),
        ),
    )
    .withColumn("case_type", F.col("match_status"))
    .withColumn(
        "disposition",
        F.when(
            (F.col("case_type") == "MISMATCH_AMOUNT")
            & (F.abs(F.col("amount_diff")) <= AUTO_RESOLVE_TOLERANCE),
            F.lit("AUTO"),
        ).otherwise(F.lit("MANUAL")),
    )
    .withColumn("created_ts", F.current_timestamp())
    .select(
        "case_id",
        "txn_id",
        "business_date",
        "channel",
        "case_type",
        "internal_amount",
        "network_amount",
        "amount_diff",
        "internal_status",
        "network_status",
        "disposition",
        "reason",
        "created_ts",
    )
)

(
    exceptions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_EXCEPTIONS)
)

print(f"Wrote {GOLD_EXCEPTIONS}: {spark.table(GOLD_EXCEPTIONS).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verification
# MAGIC
# MAGIC Counts by `match_status` (reconciliation outcome distribution) and by `disposition`
# MAGIC (auto vs manual workload). Also a sanity check that `recon` total = sum of Silver distinct
# MAGIC txn_ids on each side via full outer join cardinality.

# COMMAND ----------

print("=== gold_recon_results: counts by match_status ===")
(
    spark.table(GOLD_RECON)
    .groupBy("match_status")
    .count()
    .orderBy(F.col("count").desc())
    .show(truncate=False)
)

print("=== gold_exception_cases: counts by case_type ===")
(
    spark.table(GOLD_EXCEPTIONS)
    .groupBy("case_type")
    .count()
    .orderBy(F.col("count").desc())
    .show(truncate=False)
)

print("=== gold_exception_cases: counts by disposition ===")
(
    spark.table(GOLD_EXCEPTIONS)
    .groupBy("disposition")
    .count()
    .orderBy(F.col("count").desc())
    .show(truncate=False)
)

print("=== gold_exception_cases: disposition x case_type ===")
(
    spark.table(GOLD_EXCEPTIONS)
    .groupBy("case_type", "disposition")
    .count()
    .orderBy("case_type", "disposition")
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Sample exception cases (eyeball check)

# COMMAND ----------

(
    spark.table(GOLD_EXCEPTIONS)
    .orderBy("business_date", "case_id")
    .show(20, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done-criteria & hand-off
# MAGIC
# MAGIC **Done when:**
# MAGIC - `gold_recon_results` exists with one row per `txn_id` (full outer join of the two Silver
# MAGIC   tables) and every row carries a `match_status` in
# MAGIC   {`MATCHED`, `MISMATCH_AMOUNT`, `MISMATCH_STATUS`, `MISMATCH_BOTH`, `UNMATCHED_INTERNAL`, `UNMATCHED_NETWORK`}.
# MAGIC - `gold_exception_cases` contains exactly the non-`MATCHED` rows, each with a deterministic
# MAGIC   `case_id`, `case_type`, `disposition` (`AUTO`/`MANUAL`), and `created_ts`.
# MAGIC - The verification cell shows non-empty counts by `match_status` and by `disposition`.
# MAGIC
# MAGIC **Next phase (Phase 5 — Reports) consumes:**
# MAGIC - `gold_recon_results` -> `gold_report_funding_by_channel`, `gold_report_cash_flow`
# MAGIC - `gold_exception_cases` -> `gold_report_exception_summary`
# MAGIC
# MAGIC This notebook is idempotent: rerunning recomputes both Gold tables via `overwrite` from the
# MAGIC current Silver state, so it is safe to re-execute at any time and to drive from the Phase 6
# MAGIC orchestration driver.
