# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

storage_name = "databrick1dl"
stage_source = "bronze"
stage_des = "silver"

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# get data
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"/mnt/{storage_name}/{stage_source}/circuits.csv")

# select some column
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

#rename
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source))

#add column
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) 

#write to destination folder
circuits_final_df.write.mode("overwrite").parquet(f"/mnt/{storage_name}/{stage_des}/circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

