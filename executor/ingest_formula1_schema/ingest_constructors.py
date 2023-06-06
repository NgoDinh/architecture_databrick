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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"/mnt/{storage_name}/{stage_source}/constructors.json")

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())\
                                             .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"/mnt/{storage_name}/{stage_des}/constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")