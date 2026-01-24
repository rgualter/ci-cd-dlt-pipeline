# Databricks PySpark ETL Pipelines

Welcome to the **Databricks PySpark ETL Pipelines** project. This repository contains production-ready data engineering pipelines built using **Databricks Asset Bundles (DABs)**, **Delta Live Tables (DLT)**, and **PySpark**.

## Architecture 

The project follows a modern CI/CD workflow for Databricks:

![alt text](/docs/images/cicd-workflow.png)

## Project Structure

*   `citibike_etl/`: ETL logic for NYC Citibike data (DLT pipelines, notebooks, and scripts).
*   `formula_1_etl/`: Ingestion logic for Formula 1 historical data using the Medallion Architecture.
*   `src/`: Shared Python libraries and specialized packages (e.g., `formula1`, `citibike`).
*   `resources/`: Databricks Job and Pipeline configurations (YAML).
*   `tests/`: Unit and integration tests for shared utilities and logic.
*   `fixtures/`: Test data samples for local validation.

## Pipelines

### 1. Citibike NYC Pipeline (DLT)
A Delta Live Tables pipeline that processes NYC Citibike trip data through Bronze, Silver, and Gold layers.
- **Key Features**: Auto-scaling, data quality expectations, and seamless transition between layers.
- **Location**: `citibike_etl/`

### 2. Formula 1 historical data
A set of comprehensive ingestion notebooks that process 70+ years of Formula 1 data, organized by layer.
- **Components**: 8 specialized notebooks ingesting:
  - Circuits, Races, Constructors, Drivers, Results, Pit Stops, Lap Times, and Qualifying data.
- **Location**: `formula_1_etl/notebooks/02_silver/`

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

#### Citibike DLT Pipeline
To trigger the NYC Citibike Delta Live Tables pipeline:
```bash
databricks bundle run --target dev
```

#### Formula 1 Ingestion
To run the Formula 1 ingestion notebooks sequentially:
1. Deploy the bundle resources.
2. Trigger the job (recommended) or run the orchestrator notebook.
```bash
# Example if using the bundle job (recommended for production)
databricks bundle run formula1_ingestion_job
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
