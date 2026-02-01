from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame, SparkSession


def add_ingestion_date(input_df: DataFrame) -> DataFrame:
    """Adds a column with the current timestamp to the dataframe."""
    return input_df.withColumn("ingestion_date", current_timestamp())


def re_arrange_partition_column(
    input_df: DataFrame, partition_column: str
) -> DataFrame:
    """Moves the partition column to the end of the dataframe schema."""
    column_list = [col for col in input_df.schema.names if col != partition_column]
    column_list.append(partition_column)
    return input_df.select(column_list)


def overwrite_partition(
    input_df: DataFrame,
    db_name: str,
    table_name: str,
    partition_column: str,
    spark: SparkSession = None,
):
    """
    Overwrites a partition in a table with dynamic partition overwrite.
    If spark is not provided, it attempts to get it from the input_df.
    """
    session = spark or input_df.sparkSession
    output_df = re_arrange_partition_column(input_df, partition_column)
    session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if session.catalog.tableExists(f"{db_name}.{table_name}"):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format(
            "parquet"
        ).saveAsTable(f"{db_name}.{table_name}")


def df_column_to_list(input_df: DataFrame, column_name: str) -> list:
    """Collects distinct values from a column into a Python list."""
    df_row_list = input_df.select(column_name).distinct().collect()
    return [row[column_name] for row in df_row_list]


# =============================================================================
# Dynamic Discovery & Orchestration Utilities
# =============================================================================


def get_date_folders(dbutils, landing_path: str) -> list:
    """
    Discover all date folders in the landing zone.

    Args:
        dbutils: Databricks utilities instance
        landing_path: Path to the landing zone folder

    Returns:
        Sorted list of date folder names (e.g., ['2021-03-21', '2021-03-28'])
    """
    folders = dbutils.fs.ls(landing_path)
    return sorted(
        [
            f.name.replace("/", "")
            for f in folders
            if f.isDir() and f.name.replace("/", "")[0].isdigit()
        ]
    )


def get_layer_notebooks(dbutils, layer_path: str) -> list:
    """
    Dynamically discover notebooks in a layer folder.

    Args:
        dbutils: Databricks utilities instance
        layer_path: Path to the layer folder (e.g., bronze, silver, gold)

    Returns:
        Sorted list of notebook names without .py extension
    """
    notebooks = dbutils.fs.ls(layer_path)
    return sorted(
        [
            f.name.replace(".py", "")
            for f in notebooks
            if not f.isDir() and f.name.endswith(".py")
        ]
    )


def run_notebook_safely(
    dbutils, notebook_path: str, params: dict, timeout: int = 600
) -> tuple:
    """
    Execute a notebook with error handling and logging.

    Args:
        dbutils: Databricks utilities instance
        notebook_path: Relative path to the notebook
        params: Dictionary of parameters to pass
        timeout: Timeout in seconds (default 600)

    Returns:
        Tuple of (success: bool, result: str or error message)
    """
    try:
        result = dbutils.notebook.run(notebook_path, timeout, params)
        print(f"Success: {notebook_path}")
        return (True, result)
    except Exception as e:
        error_msg = str(e)
        print(f"Failed: {notebook_path} - {error_msg}")
        return (False, error_msg)


def validate_layer_transition(
    spark: SparkSession, source_table: str, target_table: str, key_columns: list = None
) -> dict:
    """
    Validate data integrity between source and target tables.

    Args:
        spark: SparkSession instance
        source_table: Fully qualified source table name
        target_table: Fully qualified target table name
        key_columns: Optional list of key columns to check for duplicates

    Returns:
        Dictionary with validation results
    """
    result = {
        "source_table": source_table,
        "target_table": target_table,
        "source_count": 0,
        "target_count": 0,
        "row_match": False,
        "has_duplicates": False,
    }

    try:
        source_df = spark.table(source_table)
        target_df = spark.table(target_table)

        result["source_count"] = source_df.count()
        result["target_count"] = target_df.count()
        result["row_match"] = result["source_count"] == result["target_count"]

        if key_columns:
            dup_count = (
                target_df.groupBy(key_columns).count().filter("count > 1").count()
            )
            result["has_duplicates"] = dup_count > 0

        if not result["row_match"]:
            print(
                f"Row mismatch: {source_table}={result['source_count']}, "
                f"{target_table}={result['target_count']}"
            )
        else:
            print(f"Validated: {target_table} ({result['target_count']} rows)")

    except Exception as e:
        print(f"Validation error: {str(e)}")
        result["error"] = str(e)

    return result
