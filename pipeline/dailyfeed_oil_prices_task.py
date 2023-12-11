# Databricks notebook source
databaseName = "fp_g5"
spark.sql(f"use {databaseName}")

# COMMAND ----------

rawDataSource='/mnt/g5/landingzone/favorita/'
bronze_path='/mnt/g5/bronze/'
bronzeCheckpoint = '/mnt/g5/bronze/bronze_check_point'
bronze_table_name="bronze_train"
bronze_path_schema='/mnt/g5/bronze/schema/oil_prices'

# COMMAND ----------

source_file_path='dbfs:/FileStore/shared_uploads/veg940@g.harvard.edu/oil120923.csv'
destination_file_path='/mnt/g5/landingzone/favorita/1701658271495/oil.csv'
dbutils.fs.cp(source_file_path, destination_file_path)

# COMMAND ----------

import os
from pyspark.sql.functions import input_file_name,expr
from pyspark.sql.types import ArrayType,IntegerType,StringType,DateType,StructType,StructField,DoubleType,TimestampType

# COMMAND ----------

schema = StructType([
    StructField("date", TimestampType(), True),
    StructField("dcoilwtico", DoubleType(), True)
])

# COMMAND ----------

schema_oil_prices = spark.read.format("csv").option("inferSchema", True).option("header", "true").load('/mnt/g5/landingzone/oil_prices.csv').schema

# COMMAND ----------

# MAGIC %fs ls /mnt/g5/landingzone/favorita/1701658271495

# COMMAND ----------

schema_hints = {"date": "DateType","dcoilwtico": "DoubleType"}

# COMMAND ----------

import json
data_stream_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")  # Specify CSV format
    .option("pathGlobFilter", "*oil*.csv")
    .option("header", True)    
    .schema(schema_oil_prices)
    .load(f"{rawDataSource}")    
    
)

# COMMAND ----------

display(data_stream_df)

# COMMAND ----------

# MAGIC %fs ls '/mnt/g5/bronze/oil_prices'

# COMMAND ----------

# MAGIC %fs ls '/mnt/g5/bronze/bronze_check_point/oil_prices'

# COMMAND ----------

delta_table_path = bronze_path+'/oil_prices'
oil_prices_checkpoint_location = f"{bronzeCheckpoint}/oil_prices"

query = (
    data_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", oil_prices_checkpoint_location)  # Provide a checkpoint directory
    .start(delta_table_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_oil_prices

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM bronze_oil_prices WHERE date  is null
# MAGIC

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()
