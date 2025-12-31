from databricks.connect import DatabricksSession


spark = DatabricksSession.builder.remote(cluster_id= "1226-130351-vrqn2qaq").getOrCreate()
spark.sql("select 1").show()