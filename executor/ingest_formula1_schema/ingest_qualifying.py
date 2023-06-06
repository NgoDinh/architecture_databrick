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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"/mnt/{storage_name}/{stage_source}/qualifying")

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"/mnt/{storage_name}/{stage_des}/qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")