# Databricks notebook source
storage_name = "databrick1dl"
stage_source = "silver"
stage_des = "gold"
path_source = f"/mnt/{storage_name}/{stage_source}"
path_des = f"/mnt/{storage_name}/{stage_des}"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{path_des}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("formula1.constructor_standings")

# COMMAND ----------

dbutils.notebook.exit("Success")