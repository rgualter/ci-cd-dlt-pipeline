"""Silver layer: Process races - add race_timestamp"""

import sys
from pyspark.sql.functions import col, concat, lit, to_timestamp, current_timestamp

df = spark.read.table("f1_bronze.races")

df_transformed = df.withColumn(
    "race_timestamp",
    to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"),
)

df_final = df_transformed.select(
    col("raceId").alias("race_id"),
    col("year").alias("race_year"),
    col("round"),
    col("circuitId").alias("circuit_id"),
    col("name"),
    col("race_timestamp"),
).withColumn("ingestion_date", current_timestamp())

df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "f1_silver.races"
)
