# TASK-01 — Local PySpark unit tests for the reconciliation logic

**Repo:** this repo (`build`) · **Branch:** create a new branch, open a PR to `main`
**Goal:** make the Phase 4 reconciliation logic unit-testable and prove it with local PySpark tests. Do NOT change the behavior of the existing notebooks.

## Context to read first
- `AGENT_CONTEXT.md` — project overview and conventions.
- `notebooks/04_phase4_gold_reconciliation.py` — the match-classification + disposition rules.

## What to build
1. **Extract logic** into `src/recon_logic.py` — a pure PySpark module (NO `dbutils`, NO Databricks-only calls) exposing functions that:
   - given internal + network DataFrames, return a `recon_results` DataFrame with a `match_status` column classified as one of:
     `MATCHED`, `MISMATCH_AMOUNT`, `MISMATCH_STATUS`, `MISMATCH_BOTH`, `UNMATCHED_INTERNAL`, `UNMATCHED_NETWORK`;
   - compute `amount_diff` (double) and the `disposition` = `AUTO` when `match_status == MISMATCH_AMOUNT` and `abs(amount_diff) <= AUTO_RESOLVE_TOLERANCE`, else `MANUAL`.
   - Constants: `AMOUNT_TOLERANCE = 0.01`, `AUTO_RESOLVE_TOLERANCE = 1.00`. Keep logic identical to the notebook.
2. **Tests** `tests/test_recon_logic.py` using `pytest` + a session-scoped local `SparkSession` fixture (`local[*]`). Cover:
   - exact match → `MATCHED`;
   - amount diff of 0.005 (within tol) → `MATCHED`; diff of 0.50 → `MISMATCH_AMOUNT`;
   - status differs, amounts equal → `MISMATCH_STATUS`;
   - both differ → `MISMATCH_BOTH`;
   - internal-only row → `UNMATCHED_INTERNAL`; network-only row → `UNMATCHED_NETWORK`;
   - disposition: amount_diff 0.50 → `AUTO`; amount_diff 5.00 → `MANUAL`.
3. **Dev deps** `requirements-dev.txt` with `pyspark` and `pytest`.
4. **Docs** `tests/README.md` with the exact run command.

## Acceptance criteria
- `pip install -r requirements-dev.txt && pytest -q` runs **green** in your VM before opening the PR.
- No changes to existing `notebooks/*.py` behavior (you may import from them only if it's clean; otherwise re-implement the rules in `src/recon_logic.py` and note it).
- PR description lists the files added and the test results.
