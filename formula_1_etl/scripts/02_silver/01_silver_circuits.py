"""Silver layer: Process circuits - rename columns"""

import sys
from pyspark.sql.functions import col, current_timestamp

df = spark.read.table("f1_bronze.circuits")

df_transformed = df.select(
    col("circuitId").alias("circuit_id"),
    col("circuitRef").alias("circuit_ref"),
    col("name"),
    col("location"),
    col("country"),
    col("lat").alias("latitude"),
    col("lng").alias("longitude"),
    col("alt").alias("altitude"),
).withColumn("ingestion_date", current_timestamp())

df_transformed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "f1_silver.circuits"
)
