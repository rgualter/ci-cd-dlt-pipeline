# Citibike PySpark DLT Pipeline

Welcome to the **Citibike PySpark DLT Pipeline** project. This repository contains a production-ready data engineering pipeline built using **Databricks Asset Bundles (DABs)**, **Delta Live Tables (DLT)**, and **PySpark**.

## Architecture 

![alt text](/docs/images/cicd-workflow.png)

## Project Structure

*   `src/`: Shared Python libraries and utilities.
*   `citibike_etl/`: ETL logic (DLT pipelines, notebooks, and scripts).
*   `resources/`: Databricks Job and Pipeline configurations (YAML).
*   `tests/`: Unit and integration tests.
*   `fixtures/`: Test data samples.

## Getting Started

### Prerequisites
*   **Python 3.12** (Required for Databricks Runtime 16.4 compatibility)
*   **Databricks CLI** (v0.200 or later)
*   **Java 8/11** (Required locally for PySpark testing)

### 1. Local Environment Setup

We use two separate environments to avoid dependency conflicts: one for **Unit Testing (Local PySpark)** and one for **Databricks Connect**.

#### Option A: Local PySpark (For Running Tests)
Use this environment to run unit tests locally without connecting to Databricks.

```bash
# Create and activate virtual environment
python3.12 -m venv .venv_pyspark
source .venv_pyspark/bin/activate

# Install dependencies
pip install -r requirements-pyspark.txt

# Run Tests
pytest
```

#### Option B: Databricks Connect (For Remote Execution)
Use this environment to develop and run code against your remote Databricks workspace.

```bash
# Create and activate virtual environment
python3.12 -m venv .venv_dbc
source .venv_dbc/bin/activate

# Install dependencies
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

### Running the Pipeline
To manually trigger the pipeline after deployment:
```bash
databricks bundle run --target dev
```

## Resources
*   [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
*   [Delta Live Tables Documentation](https://docs.databricks.com/data-engineering/delta-live-tables/index.html)
