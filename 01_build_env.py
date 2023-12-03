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


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Creation

# COMMAND ----------

# DBTITLE 1,Date Dimension
from pyspark.sql.functions import col, dayofmonth, dayofweek, last_day, when, date_format, to_date, lit, expr, abs, datediff
import datetime

#create date range using python
start_date = datetime.date(2012, 1, 1)
end_date = datetime.date(2023, 12, 31)
date_range = [start_date + datetime.timedelta(days=x) for x in range((end_date - start_date).days + 1)]
df_dates = spark.createDataFrame(date_range, "date")

#add additional simple columns based on calendar only
df_dates = (
  df_dates
    .withColumnRenamed("value", "date")
    .withColumn("is_payday", when((dayofmonth(col("date")) == 15) | (col("date") == last_day(col("date"))), True).otherwise(False))
    .withColumn("day_of_week", date_format(col("date"), "EEEE"))
    .withColumn("is_weekend", when(dayofweek(col("date")).isin(6, 7), True).otherwise(False))
)

#add adjustment for the earthquake on 4/16/2016
earthquake_date = to_date(lit("2016-04-16"), "yyyy-MM-dd")
df_dates = (
  df_dates
    .withColumn("is_earthquake_recovery", when((col("date") >= earthquake_date) & (col("date") <= earthquake_date + expr("interval 50 days")), True).otherwise(False))
    .withColumn("days_since_earthquake", when(col("is_earthquake_recovery"), abs(datediff(col("date"), earthquake_date))).otherwise(None))
)

#create a SQL view of this new dataframe
df_dates.createOrReplaceTempView("date_base")


# COMMAND ----------

# DBTITLE 1,silver_dim_date: date dimension with one record per date, with national holidays and earthquake recovery identified
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_dim_date;
# MAGIC
# MAGIC CREATE TABLE silver_dim_date
# MAGIC AS
# MAGIC   SELECT DISTINCT --have to dedupe the duplicate holiday entries
# MAGIC     d.*,
# MAGIC     IF(h.date IS NOT NULL AND NOT h.transferred, True, False) AS is_national_holiday,
# MAGIC     IF(h.transferred, True, False) AS is_national_holiday_transferred
# MAGIC   FROM date_base AS d
# MAGIC   LEFT OUTER JOIN bronze_holidays_events AS h ON
# MAGIC     d.date = h.date
# MAGIC     AND (h.date IS NULL or h.locale = 'National')
# MAGIC ORDER BY
# MAGIC   d.date

# COMMAND ----------

# DBTITLE 1,silver_dim_regional_holiday: special date dimension with multiple rows per date.  Includes regional holidays
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_dim_regional_holiday;
# MAGIC
# MAGIC CREATE TABLE silver_dim_regional_holiday
# MAGIC AS
# MAGIC   SELECT
# MAGIC     h.*
# MAGIC   FROM bronze_holidays_events AS h
# MAGIC   WHERE
# MAGIC     h.locale != 'National'
# MAGIC     AND NOT h.transferred
# MAGIC   ORDER BY
# MAGIC     h.date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_dim_regional_holiday

# COMMAND ----------

# DBTITLE 1,Work in progress - joined results.  TODO: repoint the bronze tables to silver tables
# MAGIC %sql
# MAGIC SELECT
# MAGIC   t.*,
# MAGIC   d.is_national_holiday,
# MAGIC   d.is_national_holiday_transferred,
# MAGIC   d.is_payday,
# MAGIC   d.is_earthquake_recovery,
# MAGIC   d.days_since_earthquake,
# MAGIC   IF(rh.date IS NOT NULL, True, False) AS is_regional_holiday,
# MAGIC   s.*
# MAGIC FROM bronze_train AS t
# MAGIC INNER JOIN bronze_stores AS s ON
# MAGIC   t.store_nbr = s.store_nbr
# MAGIC INNER JOIN silver_dim_date AS d ON
# MAGIC   t.date = d.date
# MAGIC LEFT OUTER JOIN silver_dim_regional_holiday AS rh ON
# MAGIC   t.date = rh.date
# MAGIC   AND (
# MAGIC       (rh.locale = 'Local' AND s.city = rh.locale)
# MAGIC       OR (rh.locale = 'Regional' AND s.state = rh.locale)
# MAGIC   )
