import sys
import os
import pytest

# Run the tests from the root directory
sys.path.append(os.getcwd())

# Ensure we use the Spark version from the virtual environment, not the system SPARK_HOME
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# Ensure worker and driver use the same Python executable (from the venv)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Returning a Spark Session
@pytest.fixture()
def spark():
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
    except ImportError:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        except:
            raise ImportError("Neither Databricks Session or Spark Session are available")
    return spark