"""Silver layer: Process drivers - flatten name struct"""

import sys
from pyspark.sql.functions import col, concat, lit, current_timestamp

df = spark.read.table("f1_bronze.drivers")

df_transformed = df.withColumn(
    "name", concat(col("name.forename"), lit(" "), col("name.surname"))
)

df_final = df_transformed.select(
    col("driverId").alias("driver_id"),
    col("driverRef").alias("driver_ref"),
    col("number"),
    col("code"),
    col("name"),
    col("dob"),
    col("nationality"),
).withColumn("ingestion_date", current_timestamp())

df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "f1_silver.drivers"
)
