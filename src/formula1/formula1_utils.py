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
