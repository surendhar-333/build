# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Bronze Ingestion (Auto Loader)
# MAGIC
# MAGIC **Payment Settlement & Reconciliation Lakehouse — Medallion / Bronze layer**
# MAGIC
# MAGIC This notebook incrementally ingests the landed CSV files produced by **Phase 1** using
# MAGIC Databricks **Auto Loader** (`cloudFiles`) and writes two managed Delta tables:
# MAGIC
# MAGIC | Stream | Source path | Target table |
# MAGIC |--------|-------------|--------------|
# MAGIC | internal | `INTERNAL_PATH` | `workspace.settlement_recon.bronze_internal` |
# MAGIC | network  | `NETWORK_PATH`  | `workspace.settlement_recon.bronze_network`  |
# MAGIC
# MAGIC Bronze is a **faithful, append-only copy** of the source with two audit columns added:
# MAGIC `_ingest_ts` (`current_timestamp()`) and `_source_file` (`_metadata.file_path`).
# MAGIC No cleaning, deduping, or type tightening happens here — that is Phase 3 (Silver).
# MAGIC
# MAGIC **Free Edition notes:** serverless compute; Auto Loader runs in batch-incremental mode via
# MAGIC `.trigger(availableNow=True)` (processes all currently available files, then stops — not continuous).
# MAGIC Schema and checkpoint locations live under `VOLUME_ROOT/_schemas/<t>` and
# MAGIC `VOLUME_ROOT/_checkpoints/<t>` — siblings of `internal/` and `network/` so they are never scanned as input.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared constants
# MAGIC Repeated verbatim across every phase notebook so table/column names line up.
# MAGIC If `CATALOG = "workspace"` raises a permission error on your workspace, swap it for an existing catalog.

# COMMAND ----------

# Shared constants (identical across all phase notebooks) ----------------------
CATALOG = "workspace"          # Free Edition default; swap if you hit a permission error
SCHEMA  = "settlement_recon"
VOLUME  = "landing"

VOLUME_ROOT   = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
INTERNAL_PATH = f"{VOLUME_ROOT}/internal"
NETWORK_PATH  = f"{VOLUME_ROOT}/network"

# Auto Loader bookkeeping locations (siblings of internal/ and network/, never scanned as input)
SCHEMAS_ROOT     = f"{VOLUME_ROOT}/_schemas"
CHECKPOINTS_ROOT = f"{VOLUME_ROOT}/_checkpoints"

# Fully-qualified Bronze target tables
BRONZE_INTERNAL = f"{CATALOG}.{SCHEMA}.bronze_internal"
BRONZE_NETWORK  = f"{CATALOG}.{SCHEMA}.bronze_network"

print("VOLUME_ROOT     :", VOLUME_ROOT)
print("INTERNAL_PATH   :", INTERNAL_PATH)
print("NETWORK_PATH    :", NETWORK_PATH)
print("SCHEMAS_ROOT    :", SCHEMAS_ROOT)
print("CHECKPOINTS_ROOT:", CHECKPOINTS_ROOT)
print("BRONZE_INTERNAL :", BRONZE_INTERNAL)
print("BRONZE_NETWORK  :", BRONZE_NETWORK)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure catalog & schema exist (idempotent)
# MAGIC The managed Delta tables created by `.toTable(...)` need their parent schema to exist.
# MAGIC Volumes themselves are created in Phase 1; here we only guarantee the namespace.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Namespace ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable Bronze ingestion function
# MAGIC One helper drives both streams. It:
# MAGIC 1. Reads CSV with `cloudFiles` (header `true`, schema **inference** persisted to `cloudFiles.schemaLocation`).
# MAGIC 2. Adds the two audit columns.
# MAGIC 3. Writes a managed Delta table with `.trigger(availableNow=True)` and a per-table `checkpointLocation`.
# MAGIC
# MAGIC **Schema handling**
# MAGIC - `cloudFiles.inferColumnTypes = true` lets Auto Loader infer real types from the CSVs.
# MAGIC - `cloudFiles.schemaEvolutionMode = addNewColumns` + `.option("mergeSchema", "true")` on the writer
# MAGIC   means the **network** stream's extra `network_ref` column is picked up automatically without
# MAGIC   any special-casing — the same code path serves both sources.
# MAGIC - `rescuedDataColumn = _rescued_data` captures any data that does not match the inferred schema
# MAGIC   instead of silently dropping it (kept in Bronze for traceability).
# MAGIC - We read the partition subfolder `business_date=YYYY-MM-DD` as a normal nested path. The CSV already
# MAGIC   carries a `business_date` column, so we do **not** enable partition-column inference (avoids a
# MAGIC   duplicate/typed-conflict on `business_date`).

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

def ingest_bronze(source_path: str, table_name: str, label: str):
    """Incrementally ingest CSVs from source_path into managed Delta table_name via Auto Loader."""
    schema_loc     = f"{SCHEMAS_ROOT}/{label}"
    checkpoint_loc = f"{CHECKPOINTS_ROOT}/{label}"

    print(f"[{label}] source     : {source_path}")
    print(f"[{label}] schemaLoc  : {schema_loc}")
    print(f"[{label}] checkpoint : {checkpoint_loc}")
    print(f"[{label}] target     : {table_name}")

    # --- Read side: Auto Loader (cloudFiles) ---------------------------------
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.partitionColumns", "")  # business_date is a real CSV column, not a partition to infer
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .load(source_path)
    )

    # --- Audit columns -------------------------------------------------------
    enriched = (
        reader
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    # --- Write side: managed Delta table, batch-incremental ------------------
    query = (
        enriched.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_loc)
        .option("mergeSchema", "true")          # absorb network_ref / future new columns
        .trigger(availableNow=True)             # Free Edition: process available files then stop
        .toTable(table_name)
    )

    query.awaitTermination()
    print(f"[{label}] stream finished — wrote to {table_name}\n")
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream 1 — internal -> `bronze_internal`
# MAGIC Source columns: `txn_id, business_date, channel, amount, currency, status, account_id, txn_ts`.

# COMMAND ----------

ingest_bronze(INTERNAL_PATH, BRONZE_INTERNAL, "bronze_internal")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream 2 — network -> `bronze_network`
# MAGIC Same columns as internal **plus** `network_ref`. The extra column is absorbed automatically by
# MAGIC schema inference + `mergeSchema` — no code difference from the internal stream.

# COMMAND ----------

ingest_bronze(NETWORK_PATH, BRONZE_NETWORK, "bronze_network")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification — row counts & schema
# MAGIC Confirms both Bronze tables exist, are populated, and carry the audit columns
# MAGIC (`_ingest_ts`, `_source_file`) plus — for network — `network_ref`.

# COMMAND ----------

internal_cnt = spark.table(BRONZE_INTERNAL).count()
network_cnt  = spark.table(BRONZE_NETWORK).count()

counts_df = spark.createDataFrame(
    [(BRONZE_INTERNAL, internal_cnt), (BRONZE_NETWORK, network_cnt)],
    ["table", "row_count"],
)
print(f"{BRONZE_INTERNAL}: {internal_cnt} rows")
print(f"{BRONZE_NETWORK} : {network_cnt} rows")
display(counts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema sanity check

# COMMAND ----------

print("=== bronze_internal schema ===")
spark.table(BRONZE_INTERNAL).printSchema()

print("=== bronze_network schema ===")
spark.table(BRONZE_NETWORK).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample rows (audit columns visible)

# COMMAND ----------

display(spark.table(BRONZE_INTERNAL).limit(10))

# COMMAND ----------

display(spark.table(BRONZE_NETWORK).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done-criteria & hand-off
# MAGIC
# MAGIC **This phase is complete when:**
# MAGIC - `workspace.settlement_recon.bronze_internal` and `workspace.settlement_recon.bronze_network`
# MAGIC   exist as **managed Delta** tables and have row counts > 0.
# MAGIC - Both carry audit columns `_ingest_ts` (timestamp) and `_source_file` (string), and a
# MAGIC   `_rescued_data` column for any off-schema data.
# MAGIC - `bronze_network` additionally contains the `network_ref` column.
# MAGIC - Auto Loader schema/checkpoint state is persisted under `VOLUME_ROOT/_schemas/*` and
# MAGIC   `VOLUME_ROOT/_checkpoints/*`, so re-running this notebook is **idempotent**: only new/unseen
# MAGIC   files are ingested (already-processed files are skipped via the checkpoint).
# MAGIC
# MAGIC **Re-run behaviour:** running again after Phase 1 lands more `business_date=...` folders will
# MAGIC append only the new files. To do a full clean reload, drop the two tables and delete their
# MAGIC `_schemas/<t>` and `_checkpoints/<t>` directories, then re-run.
# MAGIC
# MAGIC **Next phase (Phase 3 — Silver) consumes:**
# MAGIC `bronze_internal` and `bronze_network` and produces `silver_internal` / `silver_network`
# MAGIC (cleaned, typed, deduped; optional `*_rejects`). Silver should drop the `_rescued_data` /
# MAGIC audit columns as appropriate and enforce the typed contract on `txn_id, business_date, channel,
# MAGIC amount, currency, status, account_id, txn_ts` (+ `network_ref` for network).
