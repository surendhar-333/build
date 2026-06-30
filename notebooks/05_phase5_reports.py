# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 — Settlement Reports (Gold reporting layer)
# MAGIC
# MAGIC **Payment Settlement & Reconciliation Lakehouse — Medallion architecture**
# MAGIC
# MAGIC This notebook builds the **aggregate reporting tables** that sit on top of the cleaned
# MAGIC Silver tables and the reconciliation Gold tables produced by earlier phases.
# MAGIC
# MAGIC | Report table | Grain | Source |
# MAGIC |---|---|---|
# MAGIC | `gold_report_funding_by_channel` | business_date + channel | `silver_internal` |
# MAGIC | `gold_report_cash_flow` | business_date | `silver_internal` |
# MAGIC | `gold_report_exception_summary` | case_type + disposition | `gold_exception_cases` |
# MAGIC
# MAGIC **Inputs (do NOT regenerate):**
# MAGIC - `workspace.settlement_recon.silver_internal` (Phase 3)
# MAGIC - `workspace.settlement_recon.gold_exception_cases` (Phase 4)
# MAGIC
# MAGIC All three outputs are written as **Delta tables (overwrite)** and are fully idempotent —
# MAGIC the notebook is safe to re-run top-to-bottom on Free Edition serverless compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — shared constants
# MAGIC Repeated verbatim from the shared contract so every phase lines up on
# MAGIC catalog / schema / table names.

# COMMAND ----------

# Shared constants (identical across all phases) -----------------------------
CATALOG = "workspace"          # Free Edition default; if a permission error occurs,
                               # swap to an existing catalog you can write to.
SCHEMA  = "settlement_recon"
VOLUME  = "landing"

VOLUME_ROOT   = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
INTERNAL_PATH = f"{VOLUME_ROOT}/internal"
NETWORK_PATH  = f"{VOLUME_ROOT}/network"

# Fully-qualified table names ------------------------------------------------
# Inputs
SILVER_INTERNAL      = f"{CATALOG}.{SCHEMA}.silver_internal"
GOLD_EXCEPTION_CASES = f"{CATALOG}.{SCHEMA}.gold_exception_cases"

# Outputs (this phase)
REPORT_FUNDING   = f"{CATALOG}.{SCHEMA}.gold_report_funding_by_channel"
REPORT_CASH_FLOW = f"{CATALOG}.{SCHEMA}.gold_report_cash_flow"
REPORT_EXC_SUMM  = f"{CATALOG}.{SCHEMA}.gold_report_exception_summary"

# Make sure the catalog/schema exist and are the active context.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print("Catalog/schema ready.")
print("Inputs :", SILVER_INTERNAL, "|", GOLD_EXCEPTION_CASES)
print("Outputs:", REPORT_FUNDING, "|", REPORT_CASH_FLOW, "|", REPORT_EXC_SUMM)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports & input validation
# MAGIC Fail fast with a clear message if an upstream phase has not run yet.

# COMMAND ----------

from pyspark.sql import functions as F

def _require_table(fqname: str, producing_phase: str) -> None:
    """Raise a friendly error if a required upstream table is missing."""
    if not spark.catalog.tableExists(fqname):
        raise AssertionError(
            f"Required input table '{fqname}' not found. "
            f"Run {producing_phase} before this Phase 5 reports notebook."
        )

_require_table(SILVER_INTERNAL,      "Phase 3 (Silver)")
_require_table(GOLD_EXCEPTION_CASES, "Phase 4 (Gold reconciliation)")

print("All required input tables are present.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Report 1 — Funding by channel
# MAGIC
# MAGIC `gold_report_funding_by_channel`: one row per **business_date + channel**.
# MAGIC
# MAGIC - `settled_count`  — number of `SETTLED` internal transactions
# MAGIC - `settled_amount` — sum of amount for `SETTLED` internal transactions
# MAGIC - `total_count`    — number of internal transactions (all statuses)
# MAGIC - `total_amount`   — sum of amount across all statuses
# MAGIC
# MAGIC Source of truth is the **internal** ledger (`silver_internal`), since funding is
# MAGIC settled from our own books rather than the network feed.

# COMMAND ----------

silver_internal = spark.table(SILVER_INTERNAL)

is_settled = (F.col("status") == F.lit("SETTLED"))

funding_by_channel = (
    silver_internal
    .groupBy("business_date", "channel")
    .agg(
        # SETTLED-only metrics (conditional aggregation)
        F.sum(F.when(is_settled, F.lit(1)).otherwise(F.lit(0))).alias("settled_count"),
        F.coalesce(
            F.sum(F.when(is_settled, F.col("amount"))), F.lit(0.0)
        ).cast("double").alias("settled_amount"),
        # Totals across all statuses
        F.count(F.lit(1)).alias("total_count"),
        F.coalesce(F.sum("amount"), F.lit(0.0)).cast("double").alias("total_amount"),
    )
    .orderBy("business_date", "channel")
)

(
    funding_by_channel.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(REPORT_FUNDING)
)

print(f"Wrote {REPORT_FUNDING}")
display(spark.table(REPORT_FUNDING))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Report 2 — Daily cash flow
# MAGIC
# MAGIC `gold_report_cash_flow`: one row per **business_date**.
# MAGIC
# MAGIC **Assumption (documented):** we treat the internal ledger as the cash position.
# MAGIC - `cash_in`  = sum of amount where `status = SETTLED` (money successfully settled in).
# MAGIC - `cash_out` = sum of amount where `status IN (REVERSED, FAILED)` (money that left /
# MAGIC   never landed — reversals and failed settlements are modelled as outflow / leakage).
# MAGIC - `net`      = `cash_in - cash_out`.
# MAGIC
# MAGIC `PENDING` transactions are intentionally excluded from both sides — they are not yet
# MAGIC realised cash. This is a reasonable interpretation for a daily settlement position;
# MAGIC adjust the status buckets here if business rules differ.

# COMMAND ----------

is_cash_in  = (F.col("status") == F.lit("SETTLED"))
is_cash_out = F.col("status").isin("REVERSED", "FAILED")

cash_flow = (
    silver_internal
    .groupBy("business_date")
    .agg(
        F.coalesce(F.sum(F.when(is_cash_in, F.col("amount"))), F.lit(0.0))
            .cast("double").alias("cash_in"),
        F.coalesce(F.sum(F.when(is_cash_out, F.col("amount"))), F.lit(0.0))
            .cast("double").alias("cash_out"),
    )
    .withColumn("net", (F.col("cash_in") - F.col("cash_out")).cast("double"))
    .orderBy("business_date")
)

(
    cash_flow.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(REPORT_CASH_FLOW)
)

print(f"Wrote {REPORT_CASH_FLOW}")
display(spark.table(REPORT_CASH_FLOW))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Report 3 — Exception summary
# MAGIC
# MAGIC `gold_report_exception_summary`: one row per **case_type + disposition** drawn from
# MAGIC `gold_exception_cases` (Phase 4).
# MAGIC
# MAGIC - `case_count`       — number of exception cases in the bucket
# MAGIC - `total_amount_diff`— summed `amount_diff` impact for the bucket
# MAGIC
# MAGIC This is resilient to minor schema variation in Phase 4: if `disposition` or
# MAGIC `amount_diff` is absent we substitute safe defaults so the report still builds.

# COMMAND ----------

exc = spark.table(GOLD_EXCEPTION_CASES)
exc_cols = set(exc.columns)

# Defensive column resolution against the Phase 4 contract.
if "case_type" not in exc_cols:
    raise AssertionError(
        f"'{GOLD_EXCEPTION_CASES}' is missing required column 'case_type'. "
        f"Found columns: {sorted(exc_cols)}"
    )

disposition_col = (
    F.col("disposition") if "disposition" in exc_cols
    else F.lit("UNSPECIFIED")
)
amount_diff_col = (
    F.col("amount_diff").cast("double") if "amount_diff" in exc_cols
    else F.lit(0.0)
)

exception_summary = (
    exc
    .withColumn("disposition", disposition_col)
    .withColumn("_amount_diff", amount_diff_col)
    .groupBy("case_type", "disposition")
    .agg(
        F.count(F.lit(1)).alias("case_count"),
        F.coalesce(F.sum("_amount_diff"), F.lit(0.0)).cast("double").alias("total_amount_diff"),
    )
    .orderBy(F.col("case_count").desc(), "case_type", "disposition")
)

(
    exception_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(REPORT_EXC_SUMM)
)

print(f"Wrote {REPORT_EXC_SUMM}")
display(spark.table(REPORT_EXC_SUMM))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation summary
# MAGIC Quick row counts so a re-run gives an at-a-glance sanity check.

# COMMAND ----------

for tbl in (REPORT_FUNDING, REPORT_CASH_FLOW, REPORT_EXC_SUMM):
    print(f"{tbl:60s} rows = {spark.table(tbl).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done-criteria & hand-off
# MAGIC
# MAGIC **This phase is complete when** the following three Delta tables exist and are populated:
# MAGIC - `workspace.settlement_recon.gold_report_funding_by_channel`
# MAGIC - `workspace.settlement_recon.gold_report_cash_flow`
# MAGIC - `workspace.settlement_recon.gold_report_exception_summary`
# MAGIC
# MAGIC All three are written `mode("overwrite")` and are therefore idempotent.
# MAGIC
# MAGIC **What the next phase consumes:** Phase 6 (orchestration driver) runs this notebook as the
# MAGIC final reporting step in the medallion pipeline and records execution metrics to
# MAGIC `workspace.settlement_recon.gold_scale_log`. These report tables are the
# MAGIC presentation-layer outputs intended for BI dashboards / downstream consumers.
