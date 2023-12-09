# Databricks notebook source
from datetime import datetime, timedelta
base_path = "/mnt/g5/landingzone/favorita"
today_date = datetime.now().strftime("%Y-%m-%d")
timestamp = datetime.strptime(today_date, '%Y-%m-%d').timestamp()
timestamp_seconds = int(timestamp)
timestamp_seconds

# COMMAND ----------

daily_path=base_path+'/'+str(timestamp_seconds)+'/'
#dbutils.fs.mkdirs(daily_path)
source_file_path='dbfs:/FileStore/shared_uploads/veg940@g.harvard.edu/oil12082023.csv'
destination_file_path=daily_path+'oil12082023.csv'

# COMMAND ----------

destination_file_path

# COMMAND ----------

dbutils.fs.cp(source_file_path, destination_file_path)

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/g5/landingzone/favorita/1701993600'

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/g5/landingzone/favorita/1701993600/oil12082023.csv', True)

# COMMAND ----------

# MAGIC %fs ls '/mnt/g5/landingzone/favorita/1701993600/'

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType

# Define the schema with a specific date format
custom_schema = StructType([
    StructField("dt",StringType(), True),  # Change the name and data type accordingly
    StructField("dcoilwtico", StringType(), True)  # Adjust other columns and data types
    # Add more StructField entries for other columns as needed
])

# COMMAND ----------

daily_df = spark.read.format("csv").option("header", "true").schema(custom_schema).load(daily_path)

# COMMAND ----------

display(daily_df)

# COMMAND ----------

daily_df.createOrReplaceTempView('merge_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM merge_table

# COMMAND ----------

# Load the main oil table
oil_df = spark.read.format("delta").table("bronze_oil_prices")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_oil_prices

# COMMAND ----------

#Use Merge Delta Command to update new oil price in the main delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze_oil_prices as d
# MAGIC USING (SELECT *, CAST(dt as date) as date from merge_table) m
# MAGIC on d.date = m.dt
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *
