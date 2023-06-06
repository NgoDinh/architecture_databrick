# Databricks notebook source
excutor_folder = "executor/transform_formula1_schema"

# COMMAND ----------

dbutils.notebook.run(f"../{excutor_folder}/prepare_database", 0)

# COMMAND ----------

resul_transform_race_results = dbutils.notebook.run(f"../{excutor_folder}/transform_race_results", 0)

# COMMAND ----------

if  resul_transform_race_results == "Success":
    dbutils.notebook.run(f"../{excutor_folder}/transform_driver_standings", 0)
else:
    print("Cannot process")

# COMMAND ----------

if  resul_transform_race_results == "Success":
    dbutils.notebook.run(f"../{excutor_folder}/transform_constructor_standings", 0)
else:
    print("Cannot process")