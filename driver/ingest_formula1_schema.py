# Databricks notebook source
# source of data will be note in master file (file in driver folder)
# diver folder will run all script in excutor folder
# each schema will have a folder in excutor folder to store all script to ingest data of this schema
# excutor folder can be change so it is a dynamic and will be set when schedule job

dbutils.widgets.text("excutor_folder", "executor/ingest_formula1_schema")
excutor_folder = dbutils.widgets.get("excutor_folder")
data_source = "Ergast API""

# COMMAND ----------

dbutils.notebook.run(f"../{excutor_folder}/ingest_circuits", 0, {"p_data_source": data_source})

# COMMAND ----------

dbutils.notebook.run(f"../{excutor_folder}/ingest_races", 0, {"p_data_source": data_source})

# COMMAND ----------

dbutils.notebook.run(f"../{excutor_folder}/ingest_constructors", 0, {"p_data_source": data_source})

# COMMAND ----------

dbutils.notebook.run(f"../{excutor_folder}/ingest_drivers", 0, {"p_data_source": data_source})

# COMMAND ----------

dbutils.notebook.run(f"../{excutor_folder}/ingest_lap_times", 0, {"p_data_source": data_source})

# COMMAND ----------

dbutils.notebook.run(f"../{excutor_folder}/ingest_pit_stops", 0, {"p_data_source": data_source})

# COMMAND ----------

dbutils.notebook.run(f"../{excutor_folder}/ingest_qualifying", 0, {"p_data_source": data_source})

# COMMAND ----------

dbutils.notebook.run(f"../{excutor_folder}/ingest_results", 0, {"p_data_source": data_source})