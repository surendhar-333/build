# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 6 — Orchestration + Scale Lab
# MAGIC
# MAGIC **Project:** Payment Settlement & Reconciliation Lakehouse (Databricks Free Edition, serverless)
# MAGIC
# MAGIC This is the capstone notebook. It does two things:
# MAGIC
# MAGIC - **Part A — Orchestration:** how to wire Phases 1→5 into a single Databricks **multi-task Job**
# MAGIC   (with a sample task JSON in dependency order), plus a runnable Python **driver** that chains the
# MAGIC   phase notebooks with `dbutils.notebook.run(...)` and prints each phase's status.
# MAGIC - **Part B — Scale lab:** a `rows` widget that re-runs generation at a chosen size, times every phase
# MAGIC   with `time.time()`, `OPTIMIZE ... ZORDER BY (txn_id)` on the recon results, and appends one row per
# MAGIC   phase to `gold_scale_log` (run_rows, phase, seconds, ts) so you can compare 100K / 1M / 10M runs.
# MAGIC
# MAGIC Runnable top-to-bottom on Free Edition serverless. All notebooks are referenced by **relative name**,
# MAGIC so they must live in the **same Git folder** as this one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Config (shared constants)
# MAGIC
# MAGIC Repeated verbatim from the shared contract so this notebook is self-contained. On Free Edition the
# MAGIC default catalog is usually `workspace`; if you hit a permissions error, swap `CATALOG` for a catalog
# MAGIC you can already see in the Catalog browser and re-run.

# COMMAND ----------

# ---- Configuration (shared contract — identical across all phases) ----------
CATALOG = "workspace"            # Free Edition default; change if you lack CREATE rights
SCHEMA = "settlement_recon"
VOLUME = "landing"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
INTERNAL_PATH = f"{VOLUME_ROOT}/internal"
NETWORK_PATH = f"{VOLUME_ROOT}/network"

# Fully-qualified table names produced across the medallion pipeline
T_BRONZE_INTERNAL = f"{CATALOG}.{SCHEMA}.bronze_internal"
T_BRONZE_NETWORK = f"{CATALOG}.{SCHEMA}.bronze_network"
T_SILVER_INTERNAL = f"{CATALOG}.{SCHEMA}.silver_internal"
T_SILVER_NETWORK = f"{CATALOG}.{SCHEMA}.silver_network"
T_RECON = f"{CATALOG}.{SCHEMA}.gold_recon_results"
T_EXCEPTIONS = f"{CATALOG}.{SCHEMA}.gold_exception_cases"
T_SCALE_LOG = f"{CATALOG}.{SCHEMA}.gold_scale_log"

# Make sure the schema exists so we can write the scale log even on a clean workspace.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print("Config ready.")
print("  Catalog/Schema:", f"{CATALOG}.{SCHEMA}")
print("  Volume root   :", VOLUME_ROOT)
print("  Scale log     :", T_SCALE_LOG)

# COMMAND ----------

# MAGIC %md
# MAGIC # PART A — Orchestration
# MAGIC
# MAGIC ## A.1 — Wire Phases 1–5 as a Databricks multi-task Job
# MAGIC
# MAGIC In production you do **not** run these notebooks by hand. You create one **multi-task Job** where each
# MAGIC phase is a `notebook_task` and `depends_on` enforces the medallion order:
# MAGIC
# MAGIC ```
# MAGIC   phase1_generate  ─▶  phase2_bronze  ─▶  phase3_silver  ─▶  phase4_gold  ─▶  phase5_reports
# MAGIC ```
# MAGIC
# MAGIC **How to create it (UI):** Workflows → Create Job → add five tasks, each *Type = Notebook*, point each
# MAGIC at the matching notebook in this Git folder, and set *Depends on* to the previous task. On Free Edition
# MAGIC leave the cluster as **Serverless**. Or paste the JSON below via **Jobs API 2.1** (`POST /api/2.1/jobs/create`).
# MAGIC
# MAGIC ### Sample job spec (tasks in dependency order)
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "settlement_recon_pipeline",
# MAGIC   "tags": { "project": "settlement_recon", "env": "free-edition" },
# MAGIC   "max_concurrent_runs": 1,
# MAGIC   "parameters": [
# MAGIC     { "name": "rows", "default": "100000" }
# MAGIC   ],
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "phase1_generate",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "01_phase1_data_generation",
# MAGIC         "source": "GIT",
# MAGIC         "base_parameters": { "rows": "{{job.parameters.rows}}" }
# MAGIC       }
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "phase2_bronze",
# MAGIC       "depends_on": [ { "task_key": "phase1_generate" } ],
# MAGIC       "notebook_task": { "notebook_path": "02_phase2_bronze_autoloader", "source": "GIT" }
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "phase3_silver",
# MAGIC       "depends_on": [ { "task_key": "phase2_bronze" } ],
# MAGIC       "notebook_task": { "notebook_path": "03_phase3_silver", "source": "GIT" }
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "phase4_gold",
# MAGIC       "depends_on": [ { "task_key": "phase3_silver" } ],
# MAGIC       "notebook_task": { "notebook_path": "04_phase4_gold_reconciliation", "source": "GIT" }
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "phase5_reports",
# MAGIC       "depends_on": [ { "task_key": "phase4_gold" } ],
# MAGIC       "notebook_task": { "notebook_path": "05_phase5_reports", "source": "GIT" }
# MAGIC     }
# MAGIC   ],
# MAGIC   "git_source": {
# MAGIC     "git_url": "https://github.com/<your-org>/<your-repo>",
# MAGIC     "git_provider": "gitHub",
# MAGIC     "git_branch": "main"
# MAGIC   },
# MAGIC   "queue": { "enabled": true }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC - `depends_on` is what makes it a DAG — each phase only starts after its parent **succeeds**, so Bronze
# MAGIC   never reads before Phase 1 has landed files, etc.
# MAGIC - The job-level `rows` parameter flows into Phase 1 (and the scale lab below reads the same widget),
# MAGIC   so one knob drives the whole pipeline.
# MAGIC - `notebook_path` values are the bare notebook names because `git_source` roots them in the repo folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.2 — Runnable driver
# MAGIC
# MAGIC When you can't (or don't want to) stand up a Job, this driver chains the phases in-process via
# MAGIC `dbutils.notebook.run(name, timeout_seconds)`. Each phase is wrapped in `try/except` so one failure is
# MAGIC reported clearly and stops the chain (later phases would only fail on missing inputs anyway).
# MAGIC
# MAGIC Notebook names are **relative** — they resolve against this notebook's folder, so all six `.py`
# MAGIC notebooks must sit together in the same Git folder.

# COMMAND ----------

import time

# Phases in strict medallion dependency order. (name, timeout_seconds)
# NOTE: we pass a "rows" widget to every phase. Phase 1 currently hardcodes its row
# count (ROWS_INTERNAL); to make the scale lab actually change volume, edit ROWS_INTERNAL
# in 01_phase1_data_generation (or add a `rows` widget there). The param is harmless to
# pass regardless — notebooks ignore unknown widgets.
PHASES = [
    ("01_phase1_data_generation", 600),
    ("02_phase2_bronze_autoloader", 600),
    ("03_phase3_silver", 600),
    ("04_phase4_gold_reconciliation", 600),
    ("05_phase5_reports", 600),
]


def run_pipeline(rows: str = "100000", stop_on_error: bool = True) -> list:
    """Run phases 1..5 in order via dbutils.notebook.run, printing per-phase status.

    Returns a list of (phase, status, seconds, detail) tuples.
    """
    results = []
    for name, timeout in PHASES:
        t0 = time.time()
        try:
            # Pass rows to every phase; only Phase 1 consumes it, others ignore unknown widgets.
            ret = dbutils.notebook.run(name, timeout, {"rows": rows})
            secs = time.time() - t0
            print(f"[OK]   {name:<32} {secs:8.1f}s   -> {ret}")
            results.append((name, "OK", round(secs, 1), ret))
        except Exception as e:  # noqa: BLE001 — we want to report any failure uniformly
            secs = time.time() - t0
            print(f"[FAIL] {name:<32} {secs:8.1f}s   -> {type(e).__name__}: {e}")
            results.append((name, "FAIL", round(secs, 1), str(e)))
            if stop_on_error:
                print("Stopping pipeline — downstream phases depend on this output.")
                break
    return results


# Uncomment to run the full pipeline end-to-end from this notebook:
# pipeline_status = run_pipeline(rows="100000")
print("Driver defined. Call run_pipeline(rows='100000') to execute phases 1-5 in order.")
print("(Left commented so this notebook stays cheap to re-run; uncomment the line above to chain everything.)")

# COMMAND ----------

# MAGIC %md
# MAGIC # PART B — Scale Lab
# MAGIC
# MAGIC ## B.1 — Row-count widget
# MAGIC
# MAGIC The `rows` widget drives both the manual scale runs and the Job parameter above. Set it to `100000`,
# MAGIC then `1000000`, then `10000000` and compare the numbers in `gold_scale_log`.

# COMMAND ----------

dbutils.widgets.text("rows", "100000", "Rows to generate (scale lab)")
RUN_ROWS = int(dbutils.widgets.get("rows"))
print(f"Scale run target: {RUN_ROWS:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.2 — Scale log table + helper
# MAGIC
# MAGIC One row per (run, phase). `run_rows` lets you slice the log by experiment size, `seconds` is the wall
# MAGIC clock for that phase, `ts` is when it was recorded. Idempotent: created if missing, appended to otherwise.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType
import datetime

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {T_SCALE_LOG} (
        run_rows BIGINT,
        phase    STRING,
        seconds  DOUBLE,
        ts       TIMESTAMP
    ) USING DELTA
""")

_SCALE_LOG_SCHEMA = StructType([
    StructField("run_rows", LongType(), False),
    StructField("phase", StringType(), False),
    StructField("seconds", DoubleType(), False),
    StructField("ts", TimestampType(), False),
])


def log_phase(run_rows: int, phase: str, seconds: float) -> None:
    """Append one timing row to gold_scale_log."""
    now = datetime.datetime.now()
    row = [(int(run_rows), phase, float(round(seconds, 3)), now)]
    (
        spark.createDataFrame(row, schema=_SCALE_LOG_SCHEMA)
        .write.mode("append")
        .saveAsTable(T_SCALE_LOG)
    )
    print(f"  logged: run_rows={run_rows:,}  phase={phase:<22} seconds={seconds:8.2f}")


print("Scale log ready:", T_SCALE_LOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.3 — Timed pipeline run at the widget size
# MAGIC
# MAGIC Re-runs generation and then every downstream phase, timing each with `time.time()` and logging it.
# MAGIC Phase 1 receives `rows=RUN_ROWS` as a widget, but note Phase 1 currently hardcodes `ROWS_INTERNAL`,
# MAGIC so to truly vary volume you must edit that constant (or add a `rows` widget to Phase 1).
# MAGIC
# MAGIC On Free Edition serverless, very large sizes (10M) can take a while — that's the point of the lab.

# COMMAND ----------

def timed_run(run_rows: int, stop_on_error: bool = True) -> list:
    """Run phases 1..5, timing and logging each phase into gold_scale_log."""
    results = []
    rows_param = str(run_rows)
    for name, timeout in PHASES:
        t0 = time.time()
        try:
            ret = dbutils.notebook.run(name, timeout, {"rows": rows_param})
            secs = time.time() - t0
            print(f"[OK]   {name:<32} {secs:8.1f}s")
            log_phase(run_rows, name, secs)
            results.append((name, "OK", round(secs, 1), ret))
        except Exception as e:  # noqa: BLE001
            secs = time.time() - t0
            print(f"[FAIL] {name:<32} {secs:8.1f}s   -> {type(e).__name__}: {e}")
            log_phase(run_rows, f"{name}__FAILED", secs)
            results.append((name, "FAIL", round(secs, 1), str(e)))
            if stop_on_error:
                break
    return results


# Uncomment to execute a full timed scale run at the current widget size:
# scale_results = timed_run(RUN_ROWS)
print(f"timed_run defined. Call timed_run({RUN_ROWS}) to run + log all phases at the widget size.")
print("(Commented by default so re-running this notebook doesn't kick off the whole pipeline.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.4 — OPTIMIZE + ZORDER on the recon results
# MAGIC
# MAGIC After Gold has been built, compact the recon table and Z-order by `txn_id` (the column you look up /
# MAGIC join on most). This is itself a phase worth timing — file compaction cost scales with data volume, so
# MAGIC it belongs in the scale log too.
# MAGIC
# MAGIC Guarded by a table-exists check so this notebook still runs cleanly before Gold has ever been built.

# COMMAND ----------

def optimize_recon(run_rows: int) -> None:
    """OPTIMIZE ... ZORDER BY (txn_id) on gold_recon_results, timed and logged."""
    if not spark.catalog.tableExists(T_RECON):
        print(f"Skipping OPTIMIZE — {T_RECON} does not exist yet (run Phase 4 first).")
        return
    t0 = time.time()
    spark.sql(f"OPTIMIZE {T_RECON} ZORDER BY (txn_id)")
    secs = time.time() - t0
    print(f"[OK]   OPTIMIZE {T_RECON} ZORDER BY (txn_id)  {secs:8.1f}s")
    log_phase(run_rows, "optimize_zorder_recon", secs)


optimize_recon(RUN_ROWS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.5 — Inspect the scale log
# MAGIC
# MAGIC Pivot of seconds per phase across the run sizes you've tried. This is the table you'll be staring at
# MAGIC when you compare 100K vs 1M vs 10M.

# COMMAND ----------

if spark.catalog.tableExists(T_SCALE_LOG):
    log_df = spark.table(T_SCALE_LOG)
    print(f"=== {T_SCALE_LOG}: raw (latest 50) ===")
    log_df.orderBy(F.col("ts").desc()).show(50, truncate=False)

    print("=== seconds by phase x run_rows (avg) ===")
    (
        log_df.groupBy("phase")
        .pivot("run_rows")
        .agg(F.round(F.avg("seconds"), 1))
        .orderBy("phase")
        .show(truncate=False)
    )
else:
    print("No scale log yet — run timed_run(...) and/or optimize_recon(...) above first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## War story — record what changed across 100K / 1M / 10M
# MAGIC
# MAGIC ✅ **Phase 6 done when:** the driver chains Phases 1→5 (or the multi-task Job does), and `gold_scale_log`
# MAGIC has at least one timed run per phase for each size you tried.
# MAGIC
# MAGIC **Now write the war story.** Run the lab at `rows = 100000`, then `1000000`, then `10000000` (edit the
# MAGIC widget, uncomment `timed_run(RUN_ROWS)` in B.3, re-run B.3 → B.4). Then fill this in from `gold_scale_log`:
# MAGIC
# MAGIC - **Which phase dominated wall time at each size?** Did the bottleneck *move* (e.g. generation at 100K,
# MAGIC   but the recon join / OPTIMIZE at 10M)?
# MAGIC - **Did anything scale super-linearly?** 10x the rows but >10x the seconds usually means a shuffle/skew
# MAGIC   problem (recon join), small-file explosion, or spill. Note where.
# MAGIC - **What did `OPTIMIZE ZORDER BY (txn_id)` cost, and did it pay off** on the next run's recon join /
# MAGIC   point-lookups? Compare `optimize_zorder_recon` seconds vs the recon phase before/after.
# MAGIC - **Free Edition serverless limits you hit:** timeouts (bump the 600s?), memory spill, queueing, or the
# MAGIC   `dbutils.notebook.run` cold-start tax on the first phase of each run.
# MAGIC - **What you'd change for production:** partitioning by `business_date`, Liquid Clustering vs ZORDER,
# MAGIC   incremental Auto Loader batches instead of full regeneration, separate job clusters per phase.
# MAGIC
# MAGIC > _Your notes (100K):_ …
# MAGIC > _Your notes (1M):_ …
# MAGIC > _Your notes (10M):_ …
# MAGIC
# MAGIC This is the end of the pipeline — Phases 1–6 now form a complete, orchestrated, scale-tested
# MAGIC settlement & reconciliation lakehouse.
