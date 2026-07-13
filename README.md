<div align="center">

# 💳 Payment Settlement & Reconciliation Lakehouse

### A production-inspired Databricks lakehouse for payment settlement, exception management, reporting, and performance experiments

[![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-Data%20Engineering-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Medallion-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Tests](https://img.shields.io/badge/Tests-pytest-0A9EDC?logo=pytest&logoColor=white)](tests/test_recon_logic.py)

</div>

---

## ✨ Overview

This project models a real-world **payment settlement and reconciliation platform** on Databricks. It generates two views of payment activity—an internal ledger and a network feed—then ingests, cleans, reconciles, reports, orchestrates, and measures them through a six-phase lakehouse pipeline.

The project is designed to demonstrate practical data-engineering skills:

- 🏗️ Medallion architecture with Bronze, Silver, and Gold layers
- ⚡ Incremental ingestion with Databricks Auto Loader
- 🧹 Data quality controls, rejection tables, and deterministic deduplication
- 🔍 Full-outer-join reconciliation with six business outcomes
- 🗂️ Exception case generation with automatic/manual dispositions
- 📊 Settlement and exception reporting
- ⏱️ Job orchestration, timing telemetry, and scale experiments
- 🧪 Locally testable PySpark business logic

## 🗺️ Architecture

~~~mermaid
flowchart LR
    A["Synthetic Internal Ledger"] --> C["Unity Catalog Volume"]
    B["Synthetic Network Feed<br/>with discrepancies"] --> C

    C --> D["Bronze<br/>Auto Loader + checkpoints"]
    D --> E["Silver<br/>clean + validate + deduplicate"]
    E --> F["Gold Reconciliation<br/>full outer join + classification"]
    F --> G["Exception Cases<br/>AUTO / MANUAL"]
    E --> H["Settlement Reports"]
    G --> H
    F --> I["Scale Lab<br/>timings + OPTIMIZE / ZORDER"]

    classDef source fill:#eef6ff,stroke:#2563eb,color:#172554
    classDef bronze fill:#fff7ed,stroke:#c2410c,color:#431407
    classDef silver fill:#f8fafc,stroke:#64748b,color:#0f172a
    classDef gold fill:#fefce8,stroke:#ca8a04,color:#422006
    classDef output fill:#f0fdf4,stroke:#16a34a,color:#052e16

    class A,B,C source
    class D bronze
    class E silver
    class F,G gold
    class H,I output
~~~

## 🚀 Pipeline

| Phase | Notebook | What it does | Main outputs |
|---|---|---|---|
| 0 | [Hello Databricks](notebooks/00_hello_databricks.py) | Verifies the GitHub → Databricks Git-folder → Spark round trip | Smoke-test output |
| 1 | [Data generation](notebooks/01_phase1_data_generation.py) | Creates deterministic internal and network payment data with injected discrepancies | CSV files in a Unity Catalog volume |
| 2 | [Bronze ingestion](notebooks/02_phase2_bronze_autoloader.py) | Incrementally ingests CSV files with Auto Loader, schema tracking, rescued data, and checkpoints | <code>bronze_internal</code>, <code>bronze_network</code> |
| 3 | [Silver processing](notebooks/03_phase3_silver.py) | Standardizes types/codes, rejects invalid rows, and deduplicates transactions | <code>silver_internal</code> / <code>silver_network</code>, plus reject tables |
| 4 | [Gold reconciliation](notebooks/04_phase4_gold_reconciliation.py) | Reconciles both sides and creates analyst-ready exception cases | <code>gold_recon_results</code>, <code>gold_exception_cases</code> |
| 5 | [Reports](notebooks/05_phase5_reports.py) | Produces funding, cash-flow, and exception aggregates | Three Gold report tables |
| 6 | [Orchestration & scale](notebooks/06_phase6_orchestration_scale.py) | Chains notebooks, records runtimes, and runs optimization experiments | <code>gold_scale_log</code> |

## 🔍 Reconciliation outcomes

Every transaction is assigned one outcome:

| Outcome | Meaning | Typical action |
|---|---|---|
| ✅ <code>MATCHED</code> | Amounts agree within tolerance and statuses match | No action |
| 💰 <code>MISMATCH_AMOUNT</code> | Amount difference exceeds ₹0.01 | Auto-resolve when absolute difference ≤ ₹1.00; otherwise manual |
| 🏷️ <code>MISMATCH_STATUS</code> | Amounts agree but statuses differ | Manual review |
| ⚠️ <code>MISMATCH_BOTH</code> | Both amount and status differ | Manual review |
| 📕 <code>UNMATCHED_INTERNAL</code> | Present internally but missing from the network | Manual review |
| 📘 <code>UNMATCHED_NETWORK</code> | Present on the network but missing internally | Manual review |

Exception IDs are deterministic per business date:

<pre>CASE-&lt;business_date&gt;-&lt;8-digit sequence&gt;</pre>

## 📊 Published tables

| Layer | Tables |
|---|---|
| Bronze | <code>bronze_internal</code>, <code>bronze_network</code> |
| Silver | <code>silver_internal</code>, <code>silver_network</code>, plus reject tables |
| Gold reconciliation | <code>gold_recon_results</code>, <code>gold_exception_cases</code> |
| Gold reports | <code>gold_report_funding_by_channel</code>, <code>gold_report_cash_flow</code>, <code>gold_report_exception_summary</code> |
| Observability | <code>gold_scale_log</code> |

Default namespace:

<pre>workspace.settlement_recon</pre>

## 🛠️ Run on Databricks

### Prerequisites

- Databricks workspace with serverless compute
- Unity Catalog access
- A Databricks Git folder connected to this repository

### Run order

1. Pull the latest <code>main</code> branch into the Databricks Git folder.
2. Run <code>00_hello_databricks</code> to verify the connection.
3. Run notebooks <code>01</code> through <code>05</code> in order.
4. Open Phase 6 to create a multi-task Job or run the notebook driver.
5. Review table counts, reconciliation outcomes, and scale timings.

> **Catalog note:** the notebooks default to the <code>workspace</code> catalog. If catalog creation is restricted, replace <code>CATALOG</code> with a writable catalog available in your workspace.

Phase 1 accepts a <code>rows</code> widget. The default is 100,000 rows; Phase 6 can pass larger values for scale experiments.

## 🧪 Run tests locally

The core reconciliation rules are extracted into [src/recon_logic.py](src/recon_logic.py) and tested without Databricks-only APIs.

~~~bash
python -m venv .venv
pip install -r requirements-dev.txt
pytest -q
~~~

If Python cannot resolve the <code>src</code> package, run:

~~~bash
PYTHONPATH=. pytest -q
~~~

PowerShell equivalent:

~~~powershell
$env:PYTHONPATH = "."
pytest -q
~~~

## ⏱️ Scale lab

Phase 6 is designed to compare increasingly large runs:

| Experiment | Suggested rows | What to observe |
|---|---:|---|
| Baseline | 100,000 | Startup and notebook orchestration overhead |
| Medium | 1,000,000 | Shuffle, ingestion, and join growth |
| Large | 10,000,000 | Spill, file sizing, join pressure, and optimization cost |

It records one timing row per phase in <code>gold_scale_log</code> and can run:

~~~sql
OPTIMIZE workspace.settlement_recon.gold_recon_results
ZORDER BY (txn_id)
~~~

Use the results to explain where the bottleneck moved, whether runtime scaled linearly, and which production optimizations would matter.

## 📁 Repository structure

~~~text
build/
├── notebooks/                 # Databricks source-format notebooks, phases 0–6
├── src/
│   └── recon_logic.py         # Databricks-independent PySpark reconciliation rules
├── tests/
│   ├── test_recon_logic.py    # Local pytest coverage
│   └── README.md              # Test instructions
├── jules/
│   └── tasks/                 # AI-agent task specifications
├── AGENT_CONTEXT.md           # Detailed project handoff and conventions
├── requirements-dev.txt       # Local development dependencies
└── README.md
~~~

## 🧠 Design decisions

- **Internal ledger is the source of truth** for settlement reports.
- **Bronze stays faithful to source data** and retains audit/rescued-data columns.
- **Silver is the type and quality boundary** for downstream consumers.
- **Gold is recomputed with overwrite**, making reconciliation reruns predictable.
- **Exception disposition is rule-based**, separating small auto-resolvable amount differences from manual work.
- **Synthetic generation is deterministic**, making experiments reproducible.

## 🛣️ Next improvements

- Generate network-only and overlapping discrepancies for complete end-to-end outcome coverage
- Add GitHub Actions for local PySpark tests
- Parameterize business date and environment configuration
- Use controlled cleanup or isolated run IDs for repeatable scale comparisons
- Compare Z-Ordering with Liquid Clustering and date partitioning
- Add dashboard screenshots after the first Databricks execution

---

<div align="center">

**Built to turn payment reconciliation concepts into an executable, testable Databricks project.**

⭐ If this project helps you, consider starring the repository.

</div>
