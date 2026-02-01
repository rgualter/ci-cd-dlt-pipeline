"""Silver layer: Process constructors - rename columns"""

import sys
from pyspark.sql.functions import col, current_timestamp

df = spark.read.table("f1_bronze.constructors")

df_transformed = df.select(
    col("constructorId").alias("constructor_id"),
    col("constructorRef").alias("constructor_ref"),
    col("name"),
    col("nationality"),
).withColumn("ingestion_date", current_timestamp())

df_transformed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "f1_silver.constructors"
)
