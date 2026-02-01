# Databricks PySpark ETL Pipelines

Welcome to the **Databricks PySpark ETL Pipelines** project. This repository contains production-ready data engineering pipelines built using **Databricks Asset Bundles (DABs)**, **Delta Live Tables (DLT)**, and **PySpark**.

## Architecture 

The project follows a modern CI/CD workflow for Databricks:

![CI/CD Workflow](/docs/images/cicd-workflow.png)

## Project Structure

```
dab_project/
├── citibike_etl/               # NYC Citibike DLT pipeline
│   ├── dlt/                    # Delta Live Tables notebooks
│   │   ├── 01_bronze/          # Raw data ingestion
│   │   ├── 02_silver/          # Cleaned & enriched data
│   │   └── 03_gold/            # Business aggregations
│   ├── notebooks/              # Utility notebooks
│   └── scripts/                # Python scripts (e.g. entrypoints)
│
├── formula_1_etl/              # Formula 1 Medallion Architecture
│   └── notebooks/
│       ├── 01_bronze/          # 8 ingestion notebooks (CSV/JSON → Parquet)
│       ├── 02_silver/          # 8 transformation notebooks
│       └── 03_gold/            # 3 presentation notebooks
│
├── src/                        # Shared Python libraries
│   ├── formula1/               # Formula 1 constants & utilities
│   ├── citibike/               # Citibike utilities
│   └── utils/                  # Generic utilities
│       ├── container_cleaner.py  # ADLS/Volume cleanup utility
│       ├── mount_manager.py      # ADLS mount management
│       └── datetime_utils.py     # Date/time helpers
│
├── resources/                  # Databricks Job/Pipeline configs (YAML)
├── tests/                      # Unit tests (pytest)
├── fixtures/                   # Test data samples
├── docs/                       # Project documentation
│   ├── formula1-etl.md         # Formula 1 pipeline details
│   ├── citibike-etl.md         # Citibike DLT pipeline details
│   ├── mount_manager_examples.md  # Mount utility usage
│   └── wheel-tutorial.md       # Wheel packaging guide
│
├── .github/workflows/          # CI/CD pipelines
│   ├── ci-workflow.yml         # Continuous Integration
│   └── cd-workflow.yml         # Continuous Deployment
│
└── databricks.yml              # Databricks Asset Bundle config
```

## Pipelines

### 1. Citibike NYC Pipeline
A comprehensive pipeline that processes NYC Citibike trip data through Bronze, Silver, and Gold layers.
- **Implementations**:
  - **Delta Live Tables (DLT)**: Declarative, auto-scaling pipeline.
  - **Standard Notebooks**: Interactive/orchestrated workflow.
  - **Python Scripts**: Modular, testable production code.
- **Key Features**: Auto-scaling, data quality expectations, seamless layer transitions
- **Gold Outputs**: Daily ride summaries, station performance metrics
- **Location**: [`citibike_etl/`](./citibike_etl/)
- **Documentation**: [Citibike ETL Documentation](./docs/citibike-etl.md)

### 2. Formula 1 Historical Data Pipeline
A comprehensive ETL pipeline that processes 70+ years of Formula 1 historical data using the Medallion Architecture. it supports three implementation patterns: Notebooks, DLT, and Python Scripts.

| Layer | Notebooks | Description |
| :--- | :--- | :--- |
| **Bronze** | 8 notebooks | Raw ingestion from CSV/JSON to Parquet |
| **Silver** | 8 notebooks | Data cleansing, standardization, renaming |
| **Gold** | 3 notebooks | Race results, driver standings, constructor standings |

- **Location**: [`formula_1_etl/notebooks/`](./formula_1_etl/notebooks/)
- **Documentation**: [Formula 1 ETL Documentation](./docs/formula1-etl.md)

## Getting Started

### Prerequisites
*   **Python 3.12** (Required for Databricks Runtime 16.4 compatibility)
*   **Databricks CLI** (v0.200 or later)
*   **Java 8/11** (Required locally for PySpark testing)

### 1. Local Environment Setup

We use separate environments to avoid dependency conflicts:

#### Option A: Local PySpark (For Running Tests)
```bash
python3.12 -m venv .venv_pyspark
source .venv_pyspark/bin/activate
pip install -r requirements-pyspark.txt
pytest
```

#### Option B: Databricks Connect (For Remote Execution)
```bash
python3.12 -m venv .venv_dbc
source .venv_dbc/bin/activate
pip install -r requirements-dbc.txt
```

### 2. Databricks CLI Setup
Authenticate to your workspace:
```bash
databricks configure
```

## Deployment & CI/CD

This project uses **Databricks Asset Bundles** for deployment.

| Environment | Target | Command | Description |
| :--- | :--- | :--- | :--- |
| **Development** | `dev` | `databricks bundle deploy --target dev` | Deploys with user-prefixed resources. Pauses schedules. |
| **Production** | `prod` | `databricks bundle deploy --target prod` | Deploys release version. Schedules active. |

### Running Pipelines

### Running Pipelines

#### Citibike Pipeline
To trigger the NYC Citibike pipeline:
```bash
# 1. Run Standard Notebook Pipeline
databricks bundle run citibike_etl_pipeline_nb --target dev

# 2. Run Python Scripts Pipeline
databricks bundle run citibike_etl_pipeline_py --target dev

# 3. Run Delta Live Tables Pipeline
databricks bundle run citibike_dlt_pipeline --target dev
```

#### Formula 1 Pipeline
To run the Formula 1 ingestion pipeline:
```bash
# 1. Run Standard Notebook Pipeline
databricks bundle run formula_1_etl_pipeline_nb --target dev

# 2. Run Python Scripts Pipeline
databricks bundle run formula_1_etl_pipeline_py --target dev

# 3. Run Delta Live Tables Pipeline
databricks bundle run formula_1_etl_pipeline --target dev
```

## Testing

We use `pytest` for local validation of shared utilities.

### Running Tests
Ensure you are in the local Pyspark environment (Option A):
```bash
source .venv_pyspark/bin/activate
pytest
```

## Resources
*   [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
*   [Delta Live Tables Documentation](https://docs.databricks.com/data-engineering/delta-live-tables/index.html)
*   [Medallion Architecture Best Practices](https://www.databricks.com/glossary/medallion-architecture)

### Project Documentation
*   [Python Wheel Tutorial](./docs/wheel-tutorial.md) - How to build and update wheel packages
*   [Citibike ETL Documentation](./docs/citibike-etl.md) - NYC Citibike DLT pipeline architecture
*   [Formula 1 ETL Documentation](./docs/formula1-etl.md) - Formula 1 data processing pipeline
*   [Mount Manager Examples](./docs/mount_manager_examples.md) - ADLS Mount Manager usage
*   [Container Cleaner Examples](./docs/container_cleaner_examples.md) - ADLS/Volume cleanup utility usage
