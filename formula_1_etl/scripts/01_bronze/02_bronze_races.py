"""Bronze layer: Ingest races from CSV"""

import sys
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
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
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

df = spark.read.option("header", True).schema(schema).csv(f"{landing_path}/races.csv")

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
    "f1_bronze.races"
)
