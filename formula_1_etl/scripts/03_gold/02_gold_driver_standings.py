"""Gold layer: Create driver standings with rankings"""

import sys
from pyspark.sql.functions import col, sum, count, when, desc, rank
from pyspark.sql.window import Window

race_results_df = spark.read.table("f1_gold.race_results")

driver_standings_df = race_results_df.groupBy(
    "race_year", "driver_name", "driver_nationality"
).agg(
    sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"),
)

driver_rank_spec = Window.partitionBy("race_year").orderBy(
    desc("total_points"), desc("wins")
)

final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "f1_gold.driver_standings"
)
