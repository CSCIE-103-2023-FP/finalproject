# Databricks notebook source
# DBTITLE 1,Rebuild Database
import re
userName = spark.sql("SELECT CURRENT_USER").collect()[0]['current_user()']
userName0 = userName.split("@")[0]
userName0 = re.sub('[!#$%&\'*+-/=?^`{}|\.]+', '_', userName0)
userName1 = userName.split("@")[1]
userName = f'{userName0}@{userName1}'
dbutils.fs.mkdirs(f"/Users/{userName}/data")
userDir = f"/Users/{userName}/data"
databaseName = f"{userName0}_Final_Project"

print('databaseName ' + databaseName)
print('UserDir ' + userDir)

spark.sql(f"DROP DATABASE IF EXISTS {databaseName} CASCADE")
spark.sql(f"CREATE DATABASE {databaseName}")
spark.sql(f"use {databaseName}")

print (f"Database {databaseName} successfully rebuilt.")

# COMMAND ----------

# DBTITLE 1,Load Bronze Tables
rootPath = "dbfs:/mnt/data/2023-kaggle-final/store-sales/"

for file in dbutils.fs.ls(rootPath):
  tableName = "bronze_" + file.name.replace('.csv', '')
  print (f"processing file {file.name} into table name {tableName}...")

  loadDf = spark.read.option("header", True).option("inferSchema", True).csv(file.path)
  loadDf.write.saveAsTable(tableName) #saves delta table
  
  print(f"Successfully saved delta table {tableName}.")
  print("")

