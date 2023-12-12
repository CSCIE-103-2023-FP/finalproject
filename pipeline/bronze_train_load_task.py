# Databricks notebook source
databaseName = "fp_g5"
spark.sql(f"use {databaseName}")

# COMMAND ----------

rawDataSource='/mnt/g5/landingzone'
bronze_path='/mnt/g5/bronze/'
bronzeCheckpoint = '/mnt/g5/bronze/bronze_check_point'
bronze_table_name="bronze_train"

# COMMAND ----------

import os
from pyspark.sql.functions import input_file_name,expr
from pyspark.sql.types import ArrayType,IntegerType,StringType

# COMMAND ----------

schema_train = spark.read.format("csv").option("inferSchema", True).option("header", "true").load(f"{rawDataSource}/train.csv").schema

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("pathGlobFilter", "*train*.csv") \
  .option("header", True) \
  .schema(schema_train) \
  .load(f"{rawDataSource}") \
  #.withColumn("filename",input_file_name())
     

# COMMAND ----------

train_checkpoint_location = f"{bronzeCheckpoint}/train"
bronze_train_deltapath=bronze_path+'/train'
dbutils.fs.rm(train_checkpoint_location, True) #reset checkpoint so it reloads the file
df.writeStream.option("path", bronze_train_deltapath).outputMode("append").format("delta").option("checkpointLocation", train_checkpoint_location).option("mergeSchema", "true").trigger(availableNow=True).table(bronze_table_name)
     

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  bronze_train 
