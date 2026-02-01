"""Silver layer: Process lap times - rename columns"""

import sys
from pyspark.sql.functions import current_timestamp

df = spark.read.table("f1_bronze.lap_times")

df_final = (
    df.withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumn("ingestion_date", current_timestamp())
)

df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "f1_silver.lap_times"
)
