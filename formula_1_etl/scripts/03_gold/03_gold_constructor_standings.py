"""Gold layer: Create constructor standings with rankings"""

import sys
from pyspark.sql.functions import col, sum, count, when, desc, rank
from pyspark.sql.window import Window

race_results_df = spark.read.table("f1_gold.race_results")

constructor_standings_df = race_results_df.groupBy("race_year", "team").agg(
    sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"),
)

constructor_rank_spec = Window.partitionBy("race_year").orderBy(
    desc("total_points"), desc("wins")
)

final_df = constructor_standings_df.withColumn(
    "rank", rank().over(constructor_rank_spec)
)

final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "f1_gold.constructor_standings"
)
