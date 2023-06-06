# Databricks notebook source

from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

storage_name = "databrick1dl"
stage_source = "bronze"
stage_des = "silver"

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"/mnt/{storage_name}/{stage_source}/lap_times")

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"/mnt/{storage_name}/{stage_des}/lap_times")

# COMMAND ----------

dbutils.notebook.exit("Success")