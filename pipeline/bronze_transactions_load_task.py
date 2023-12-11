# Databricks notebook source
databaseName = "fp_g5"
spark.sql(f"use {databaseName}")

# COMMAND ----------

rawDataSource='/mnt/g5/landingzone'
bronze_path='/mnt/g5/bronze/'
bronzeCheckpoint = '/mnt/g5/bronze/bronze_check_point'
bronze_table_name="bronze_transactions"

# COMMAND ----------

import os
from pyspark.sql.functions import input_file_name,expr
from pyspark.sql.types import ArrayType,IntegerType,StringType

# COMMAND ----------

schema_transactions = spark.read.format("csv").option("inferSchema", True).option("header", "true").load(f"{rawDataSource}/transactions.csv").schema

# COMMAND ----------

transactions_df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("pathGlobFilter", "*transactions*.csv") \
  .option("header", True) \
  .schema(schema_transactions) \
  .load(f"{rawDataSource}") \
  #.withColumn("filename",input_file_name())

# COMMAND ----------

transactions_checkpoint_location = f"{bronzeCheckpoint}/transactions"
bronze_transactions_deltapath=bronze_path+'/transactions'
dbutils.fs.rm(transactions_checkpoint_location, True) #reset checkpoint so it reloads the file
transactions_df.writeStream.option("path", bronze_transactions_deltapath).outputMode("append").format("delta").option("checkpointLocation", transactions_checkpoint_location).option("mergeSchema", "true").trigger(availableNow=True).table(bronze_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM bronze_transactions
