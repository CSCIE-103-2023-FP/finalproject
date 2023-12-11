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
    StructField("date", DateType(), True),
    StructField("dcoilwtico", DoubleType(), True)
])

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
    .schema(schema)
    .load(f"{rawDataSource}")    
    
)

# COMMAND ----------

delta_table_path = bronze_path+'/oil_prices'
oil_prices_checkpoint_location = f"{bronzeCheckpoint}/oil_prices"

query = (
    data_stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", oil_prices_checkpoint_location)
    .trigger(availableNow=True)
    .start(delta_table_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze_oil_prices as d
# MAGIC USING (SELECT *, CAST(dt as date) as date from merge_table) m
# MAGIC on d.date = m.dt
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *
