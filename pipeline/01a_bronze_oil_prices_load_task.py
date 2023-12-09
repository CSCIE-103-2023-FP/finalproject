# Databricks notebook source
databaseName = "fp_g5"
spark.sql(f"use {databaseName}")

# COMMAND ----------

rawDataSource='/mnt/g5/landingzone'
bronze_path='/mnt/g5/bronze/'
bronzeCheckpoint = '/mnt/g5/bronze/bronze_check_point'
bronze_table_name="bronze_oil_prices"

# COMMAND ----------

schema_oil_prices = spark.read.format("csv").option("inferSchema", True).option("header", "true").load(f"{rawDataSource}/oil_prices.csv").schema

# COMMAND ----------

import os
from pyspark.sql.functions import input_file_name,expr
from pyspark.sql.types import ArrayType,IntegerType,StringType

# COMMAND ----------

oil_prices_df  = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("pathGlobFilter", "*oil_prices*.csv") \
  .option("header", True) \
  .schema(schema_oil_prices) \
  .load(f"{rawDataSource}") \
  #.withColumn("filename",input_file_name())

# COMMAND ----------

oil_prices_checkpoint_location = f"{bronzeCheckpoint}/oil_prices"
bronze_oil_prices_deltapath=bronze_path+'/oil_prices'
dbutils.fs.rm(oil_prices_checkpoint_location, True) #reset checkpoint so it reloads the file
oil_prices_df.writeStream.option("path", bronze_oil_prices_deltapath).outputMode("append").format("delta").option("checkpointLocation", oil_prices_checkpoint_location).option("mergeSchema", "true").table(bronze_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_oil_prices
