# jules/ — task specs for Google Jules

Workflow (keeps Claude usage minimal; Jules does the coding):

1. **Claude** writes a task file in `jules/tasks/` and pushes it to GitHub.
2. **You** (on your own laptop, where Jules is reachable) open jules.google.com, select the `build` repo, and paste a one-liner:
   > Complete the task described in `jules/tasks/TASK-XX-*.md`. Work on a new branch and open a PR to `main`.
3. **Jules** does the work in its cloud VM and opens a PR.
4. **Claude** reviews the PR diff (cheap) and tells you whether to merge.

No need to copy long specs from chat — the spec lives in the repo, Jules reads it there.

## Task index
- `tasks/TASK-01-recon-tests.md` — local PySpark unit tests for the reconciliation logic. **(ready)**
