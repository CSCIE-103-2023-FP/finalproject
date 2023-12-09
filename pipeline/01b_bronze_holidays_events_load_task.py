# Databricks notebook source
databaseName = "fp_g5"
spark.sql(f"use {databaseName}")

# COMMAND ----------

rawDataSource='/mnt/g5/landingzone'
bronze_path='/mnt/g5/bronze/'
bronzeCheckpoint = '/mnt/g5/bronze/bronze_check_point'
bronze_table_name="bronze_holidays_events"

# COMMAND ----------

import os
from pyspark.sql.functions import input_file_name,expr
from pyspark.sql.types import ArrayType,IntegerType,StringType

# COMMAND ----------

schema_holidays_events = spark.read.format("csv").option("inferSchema", True).option("header", "true").load(f"{rawDataSource}/holidays_events.csv").schema

# COMMAND ----------

holidays_events_df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("pathGlobFilter", "*holidays_events*.csv") \
  .option("header", True) \
  .schema(schema_holidays_events) \
  .load(f"{rawDataSource}") \
  #.withColumn("filename",input_file_name())

# COMMAND ----------

holidays_events_checkpoint_location = f"{bronzeCheckpoint}/holidays_events"
bronze_holidays_events_deltapath=bronze_path+'/holidays_events'
dbutils.fs.rm(holidays_events_checkpoint_location, True) #reset checkpoint so it reloads the file
holidays_events_df.writeStream.option("path", bronze_holidays_events_deltapath).outputMode("append").format("delta").option("checkpointLocation", holidays_events_checkpoint_location).option("mergeSchema", "true").table(bronze_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_holidays_events
