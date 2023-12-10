# Databricks notebook source
import re
userName = spark.sql("SELECT CURRENT_USER").collect()[0]['current_user()']
userName0 = userName.split("@")[0]
userName0 = re.sub('[!#$%&\'*+-/=?^`{}|\.]+', '_', userName0)
userName1 = userName.split("@")[1]
userName = f'{userName0}@{userName1}'
dbutils.fs.mkdirs(f"/Users/{userName}/data")
userDir = f"/Users/{userName}/data"
databaseName = f"{userName0}_FinalProject_01"

print('databaseName ' + databaseName)
print('UserDir ' + userDir)

spark.sql(f"DROP DATABASE IF EXISTS {databaseName} CASCADE")
spark.sql(f"CREATE DATABASE {databaseName}")

print (f"Database {databaseName} successfully rebuilt.")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE fp_g5;

# COMMAND ----------

tablesDf = spark.sql("SHOW TABLES")

for row in tablesDf.collect():
  sql = f"""
    CREATE OR REPLACE TABLE {databaseName}.{row.tableName}
    DEEP CLONE fp_g5.{row.tableName}
  """
  print(sql)
  spark.sql(sql)
