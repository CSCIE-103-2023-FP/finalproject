# Databricks notebook source
databaseName = "fp_g5"
spark.sql(f"use {databaseName}")

# COMMAND ----------

rawDataSource='/mnt/g5/landingzone'
bronze_path='/mnt/g5/bronze/'
bronzeCheckpoint = '/mnt/g5/bronze/bronze_check_point'
bronze_table_name="bronze_stores"

# COMMAND ----------

import os
from pyspark.sql.functions import input_file_name,expr
from pyspark.sql.types import ArrayType,IntegerType,StringType

# COMMAND ----------

schema_stores = spark.read.format("csv").option("inferSchema", True).option("header", "true").load(f"{rawDataSource}/stores.csv").schema

# COMMAND ----------

stores_df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("pathGlobFilter", "*stores*.csv") \
  .option("header", True) \
  .schema(schema_stores) \
  .load(f"{rawDataSource}") \
  #.withColumn("filename",input_file_name())

# COMMAND ----------

stores_checkpoint_location = f"{bronzeCheckpoint}/stores"
stores_transactions_deltapath=bronze_path+'/stores'
dbutils.fs.rm(stores_checkpoint_location, True) #reset checkpoint so it reloads the file
stores_df.writeStream.option("path", stores_transactions_deltapath).outputMode("append").format("delta").option("checkpointLocation", stores_checkpoint_location).option("mergeSchema", "true").trigger(availableNow=True).table(bronze_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_stores
