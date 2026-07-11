# Tests

This directory contains the local PySpark unit tests for the gold reconciliation logic.

## How to run the tests

1. Ensure you have the required dependencies installed (e.g. inside a virtual environment):
   ```bash
   pip install -r ../requirements-dev.txt
   ```

2. Run `pytest` from the root of the repository (with `PYTHONPATH` set to the current directory so it can find `src`):
   ```bash
   PYTHONPATH=. pytest -q tests/test_recon_logic.py
   ```
