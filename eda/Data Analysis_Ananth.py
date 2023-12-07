# Databricks notebook source
# MAGIC %md
# MAGIC # Final Project - Exploratory Data Analysis
# MAGIC * This notebook explores the data set for the Final Project
# MAGIC * Currently there are 3 sections one for each use case

# COMMAND ----------

# MAGIC %fs ls /mnt/data/2023-kaggle-final

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store-Sales Data Set

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("dbfs:/mnt/data/2023-kaggle-final/store-sales/")

# COMMAND ----------

holiday_events_df = spark.read.option("header", True).option("inferSchema",True).csv("dbfs:/mnt/data/2023-kaggle-final/store-sales/holidays_events.csv")
oil_df = spark.read.option("header", True).option("inferSchema",True).csv("dbfs:/mnt/data/2023-kaggle-final/store-sales/oil.csv")
stores_df = spark.read.option("header", True).option("inferSchema",True).csv("dbfs:/mnt/data/2023-kaggle-final/store-sales/stores.csv")
transactions_df = spark.read.option("header", True).option("inferSchema",True).csv("dbfs:/mnt/data/2023-kaggle-final/store-sales/transactions.csv")

# COMMAND ----------

holiday_events_df.show(5)

# COMMAND ----------

oil_df.show(5) # oil prices on that day

# COMMAND ----------

stores_df.show(5)

# COMMAND ----------

transactions_df.show(5)

# COMMAND ----------

holiday_events_df.createTempView("holiday_events")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(`date`) FROM holiday_events LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT extract( YEAR from `date`) as Yr, `locale` , COUNT(*) from holiday_events group by Yr,`locale` order by Yr

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT extract( YEAR from `date`) as Yr, COUNT(*) from holiday_events group by Yr order by Yr

# COMMAND ----------

# MAGIC %md
# MAGIC * there are 7 files `holidays_events.csv`, `oil.csv`,`sample_submission.csv`,`stores.csv`,`test.csv`, `train.csv`, `transactions.csv`
# MAGIC * all of them are `csv` type
# MAGIC * Timeline years is between 2013 and 2017 (including )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Holiday Events Observations
# MAGIC * Most of the holidays are **Local** and **National** holidays, very less on **Regional**
# MAGIC * Partition can be based on Year part of the date

# COMMAND ----------

oil_df.createTempView("oil")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM oil LIMIT 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt_of_records FROM oil;
# MAGIC SELECT extract(YEAR FROM `date`) as yr FROM oil GROUP BY yr order by yr desc;
# MAGIC

# COMMAND ----------

transactions_df.createTempView("transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM transactions LIMIT 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `store_nbr` , count(*) FROM transactions GROUP BY `store_nbr` ORDER BY 1

# COMMAND ----------

store_nbr = spark.sql("select distinct(`store_nbr`) from transactions").rdd.map(lambda row : row[0]).collect()
store_nbr.sort()
dbutils.widgets.dropdown("store_numb", "1", [str(x) for x in store_nbr])

# COMMAND ----------

databaseName = "final_project_group_5"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")
spark.sql(f"use {databaseName}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE CATALOG IF NOT EXISTS final_project_group_5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (concat(extract(YEAR from `date`),"-", lpad(extract(MONTH from `date`),2,'0'))) as yr_mnt, sum(`transactions`) FROM transactions where store_nbr = getArgument("store_numb") group by yr_mnt order by yr_mnt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Energy-prediction Data Set

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## NFL Data Set

# COMMAND ----------


