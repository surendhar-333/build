# AGENT_CONTEXT.md — read this first

> **Purpose of this file:** a self-contained handoff so any AI assistant (GitHub Copilot, Claude/Opus 4.8,
> or a fresh Claude Code session) can pick up this project and continue **without re-deriving anything**.
> Two agents may work this repo in parallel — see **Coordination** at the bottom to avoid clobbering each other.

---

## What we're building

A **Payment Settlement & Reconciliation Lakehouse** on Databricks (Free Edition, serverless), modeled on
real Fiserv-style settlement work so the owner can speak to it credibly in interviews. It is deliberately
designed to teach behavior **at scale**, not just to be a resume bullet.

### End-to-end shape (medallion architecture)

1. **Data generation** — synthetic *internal* debit records (source of truth) + a *network/bank* side derived
   from them with deliberate discrepancies injected (missing records, amount mismatches, status mismatches).
   Plain PySpark `spark.range()` so it scales to millions of rows with zero dependencies.
2. **Bronze** — land files in a Unity Catalog volume; ingest incrementally with **Auto Loader** (`cloudFiles`)
   into raw Delta tables with checkpointing.
3. **Silver** — clean, standardize, deduplicate, type-cast both sides; handle nulls, bad amounts, late files.
4. **Gold — the reconciliation engine (centerpiece)** — match internal vs network records; classify each as
   `matched` / `mismatched` / `unmatched`; write exceptions to a **cases** table with generated case IDs and an
   auto/manual disposition based on tolerance rules (mirrors an eResXpress-style flow).
5. **Reports** — aggregate into settlement reports: daily funding by channel, cash-in/cash-out, exception
   summary (mirrors an FSAMS-style report).
6. **Orchestration + scale lab** — wire it as a Databricks Job, then crank volume (1M → 10M+ rows) and tune
   partitioning / `OPTIMIZE` / Z-ordering, **logging what changes runtime**. That log is the interview material.

Build in **phases**, each a complete, stoppable checkpoint.

---

## How this repo syncs to Databricks (important)

There is **no direct CLI/token push** to the workspace. The bridge is **git**:

```
edit notebooks here  →  git commit + push  →  user clicks "Pull" in the Databricks Git folder  →  runs there
```

- Notebooks are **Databricks source format** `.py` files: first line `# Databricks notebook source`,
  cells separated by `# COMMAND ----------`, markdown via `# MAGIC %md`. Keep that format for anything meant
  to render as a notebook in Databricks.
- Repo is **private**: `https://github.com/surendhar-333/build.git`.
- The user runs cells in Databricks Free Edition (default catalog usually `workspace`) and reports back
  row counts / errors. Agents do **not** have access to the running workspace — you cannot execute cells.

---

## Current state (update this section as you go)

- ✅ `notebooks/00_hello_databricks.py` — smoke test, proves the git→pull→run round-trip.
- ✅ `notebooks/01_phase1_data_generation.py` — **Phase 1** (config / generator functions / generate+land).
  - Cell 1: catalog/schema/volume config (`workspace.settlement_recon.landing`).
  - Cell 2: `build_internal()` + `build_network()` (drop 5%, amount-mismatch 2%, status-mismatch 2%).
  - Cell 3: generate one business day (`2026-06-30`, 100K internal rows) and land CSV to the volume.
- ⏳ **Awaiting user run of Phase 1.** Expected: internal ≈ 100,000; network slightly fewer (~95,000);
  samples look like real transactions. Known snag: if `CREATE CATALOG` fails on Free Edition (permissions),
  switch `CATALOG` in Cell 1 to a pre-existing catalog from the Catalog browser and re-run.

## Next up — Phase 2 (Bronze, Auto Loader)

Point `cloudFiles` at `/Volumes/workspace/settlement_recon/landing/internal/` and `.../network/`, infer/enforce
schema, stream incrementally into `bronze_internal` and `bronze_network` Delta tables with checkpoint locations
under the volume. Add `_ingest_ts` and `_source_file` (`_metadata.file_path`) audit columns. Keep it a separate
notebook: `notebooks/02_phase2_bronze_autoloader.py`.

### Remaining roadmap
- Phase 3: `03_phase3_silver.py` — clean/standardize/dedupe both sides.
- Phase 4: `04_phase4_gold_reconciliation.py` — match + classify + exception cases table.
- Phase 5: `05_phase5_reports.py` — settlement/funding/exception aggregates.
- Phase 6: `06_phase6_orchestration_scale.py` + a Databricks Job; scale to 1M/10M and log tuning results.

---

## Conventions

- One notebook per phase, numbered `NN_phaseN_*.py`, under `notebooks/`.
- PySpark first; SQL where it reads cleaner. Idempotent DDL (`CREATE ... IF NOT EXISTS`, `mode("overwrite")`).
- Config constants live at the top of each notebook (catalog/schema/volume/paths) — keep them consistent across
  phases (`workspace.settlement_recon`, volume `landing`).
- Commit messages: short imperative, one phase per commit where possible.

## Coordination (parallel agents — read before committing)

To avoid two agents clobbering each other on the same repo:
1. **Always `git pull --rebase` before you start and before you push.**
2. Prefer **new files per phase** over editing shared ones, so changes rarely overlap.
3. After finishing a unit of work, **update the "Current state" section above** in the same commit so the other
   agent sees what's done.
4. If you hit a merge conflict in this file, keep **both** agents' status lines — never delete the other's notes.
