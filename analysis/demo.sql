-- Databricks notebook source
show databases;

-- COMMAND ----------

Use formula1;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from race_results limit 10

-- COMMAND ----------

SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(11-position) AS total_points,
       AVG(11-position) AS avg_points
  FROM race_results
WHERE position <= 10
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

