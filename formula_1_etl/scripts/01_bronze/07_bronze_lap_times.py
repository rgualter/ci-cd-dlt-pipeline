"""Bronze layer: Ingest lap times from CSV folder"""

import sys
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import create_map, lit, current_timestamp

pipeline_id = sys.argv[1]
run_id = sys.argv[2]
task_id = sys.argv[3]
processed_timestamp = sys.argv[4]
file_date = sys.argv[5] if len(sys.argv) > 5 else "2021-03-21"

landing_path = f"/mnt/gualterformula1dl/landing/{file_date}"

schema = StructType(
    [
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), True),
        StructField("lap", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
    ]
)

df = spark.read.schema(schema).csv(f"{landing_path}/lap_times")

df = df.withColumn(
    "metadata",
    create_map(
        lit("pipeline_id"),
        lit(pipeline_id),
        lit("run_id"),
        lit(run_id),
        lit("task_id"),
        lit(task_id),
        lit("processed_timestamp"),
        lit(processed_timestamp),
    ),
).withColumn("ingestion_date", current_timestamp())

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "f1_bronze.lap_times"
)
