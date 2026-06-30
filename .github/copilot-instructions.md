# Copilot / AI agent instructions

**Before doing anything, read [`AGENT_CONTEXT.md`](../AGENT_CONTEXT.md) in the repo root.** It is the source of
truth for what we're building, the current state, the next phase, and conventions.

Quick summary:
- Project: a **Payment Settlement & Reconciliation Lakehouse** on Databricks Free Edition (medallion:
  Bronze → Silver → Gold reconciliation → Reports → orchestration/scale lab). Built in phases.
- Notebooks are **Databricks source `.py`** files: line 1 `# Databricks notebook source`, cells split by
  `# COMMAND ----------`, markdown via `# MAGIC %md`. One notebook per phase under `notebooks/`.
- Sync path: edit here → commit → push → user clicks **Pull** in the Databricks Git folder. Agents cannot
  run cells in the workspace; the user runs them and reports counts/errors.
- This repo may be worked by two agents in parallel: **`git pull --rebase` before starting and before
  pushing**, prefer new files per phase, and **update the "Current state" section of `AGENT_CONTEXT.md`** in
  the same commit as your work.

Current status & next step are always at the bottom of `AGENT_CONTEXT.md` — check there.
