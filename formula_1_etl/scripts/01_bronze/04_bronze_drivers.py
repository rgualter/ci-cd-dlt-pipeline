"""Bronze layer: Ingest drivers from JSON"""

import sys
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import create_map, lit, current_timestamp

pipeline_id = sys.argv[1]
run_id = sys.argv[2]
task_id = sys.argv[3]
processed_timestamp = sys.argv[4]
file_date = sys.argv[5] if len(sys.argv) > 5 else "2021-03-21"

landing_path = f"/mnt/gualterformula1dl/landing/{file_date}"

name_schema = StructType(
    [
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True),
    ]
)

schema = StructType(
    [
        StructField("driverId", IntegerType(), False),
        StructField("driverRef", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", name_schema),
        StructField("dob", DateType(), True),
        StructField("nationality", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

df = spark.read.schema(schema).json(f"{landing_path}/drivers.json")

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
    "f1_bronze.drivers"
)
