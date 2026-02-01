"""Gold layer: Create race results by joining silver tables"""

import sys
from pyspark.sql.functions import col, current_timestamp

# Read silver tables
results_df = spark.read.table("f1_silver.results").withColumnRenamed(
    "race_id", "result_race_id"
)

drivers_df = (
    spark.read.table("f1_silver.drivers")
    .withColumnRenamed("number", "driver_number")
    .withColumnRenamed("name", "driver_name")
    .withColumnRenamed("nationality", "driver_nationality")
)

constructors_df = spark.read.table("f1_silver.constructors").withColumnRenamed(
    "name", "team"
)

circuits_df = spark.read.table("f1_silver.circuits").withColumnRenamed(
    "location", "circuit_location"
)

races_df = (
    spark.read.table("f1_silver.races")
    .withColumnRenamed("name", "race_name")
    .withColumnRenamed("race_timestamp", "race_date")
)

# Join races to circuits
race_circuits_df = races_df.join(
    circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner"
).select(
    races_df.race_id,
    races_df.race_year,
    races_df.race_name,
    races_df.race_date,
    circuits_df.circuit_location,
)

# Join results to all other dataframes
race_results_df = (
    results_df.join(
        race_circuits_df, results_df.result_race_id == race_circuits_df.race_id
    )
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id)
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)
)

final_df = race_results_df.select(
    "race_id",
    "race_year",
    "race_name",
    "race_date",
    "circuit_location",
    "driver_name",
    "driver_number",
    "driver_nationality",
    "team",
    "grid",
    "fastest_lap",
    col("time").alias("race_time"),
    "points",
    "position",
).withColumn("created_date", current_timestamp())

final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "f1_gold.race_results"
)
