# Databricks notebook source
# MAGIC %md
# MAGIC ## Write and read delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS delta_lake_demo
# MAGIC LOCATION '/mnt/databrick1dl/demo'

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/databrick1dl/bronze/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("delta_lake_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_lake_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/databrick1dl/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table delta_lake_demo.results_external

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE delta_lake_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/databrick1dl/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_lake_demo.results_external

# COMMAND ----------

display(spark.read.format("delta").load("/mnt/databrick1dl/demo/results_external"))

# COMMAND ----------

display(spark.read.format("delta").load("/mnt/databrick1dl/demo/results_managed"))

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("delta_lake_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS delta_lake_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ## update and delete

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_lake_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta_lake_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_lake_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/databrick1dl/demo/results_managed")

deltaTable.update("position <= 10", { "points": "21 - position" } ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_lake_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM delta_lake_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_lake_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/databrick1dl/demo/results_managed")

deltaTable.delete("points = 0") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_lake_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/databrick1dl/bronze/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/databrick1dl/bronze/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/databrick1dl/bronze/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta_lake_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md Day1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta_lake_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql SELECT * FROM delta_lake_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta_lake_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql SELECT * FROM delta_lake_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Day 3

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/databrick1dl/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql SELECT * FROM delta_lake_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC ## History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY delta_lake_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_lake_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete history
# MAGIC VACUUM delta_lake_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_lake_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# %sql
# SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta_lake_demo.drivers_txn (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY delta_lake_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO delta_lake_demo.drivers_txn
# MAGIC SELECT * FROM delta_lake_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY delta_lake_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO delta_lake_demo.drivers_txn
# MAGIC SELECT * FROM delta_lake_demo.drivers_merge
# MAGIC WHERE driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM  delta_lake_demo.drivers_txn
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta_lake_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO delta_lake_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM delta_lake_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA delta_lake_demo.drivers_convert_to_delta

# COMMAND ----------

