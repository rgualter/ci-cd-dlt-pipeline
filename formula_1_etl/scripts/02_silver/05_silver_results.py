"""Silver layer: Process results - rename columns and deduplicate"""

import sys
from pyspark.sql.functions import current_timestamp

df = spark.read.table("f1_bronze.results")

df_renamed = (
    df.withColumnRenamed("resultId", "result_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("positionText", "position_text")
    .withColumnRenamed("positionOrder", "position_order")
    .withColumnRenamed("fastestLap", "fastest_lap")
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
    .drop("statusId")
)

df_final = df_renamed.dropDuplicates(["race_id", "driver_id"]).withColumn(
    "ingestion_date", current_timestamp()
)

df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "f1_silver.results"
)
