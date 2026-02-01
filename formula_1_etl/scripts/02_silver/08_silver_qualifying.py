"""Silver layer: Process qualifying - rename columns"""

import sys
from pyspark.sql.functions import current_timestamp

df = spark.read.table("f1_bronze.qualifying")

df_final = (
    df.withColumnRenamed("qualifyId", "qualify_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumn("ingestion_date", current_timestamp())
)

df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "f1_silver.qualifying"
)
