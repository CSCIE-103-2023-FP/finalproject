# Databricks notebook source
# MAGIC %md
# MAGIC # Batch and Structured Streaming for Silver dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_product_family
# MAGIC * Treat this as a reference table of product families
# MAGIC * daily/ batch approach would be to check if there are any new family of products which are not there
# MAGIC   * if not found insert
# MAGIC   * if found ignore
# MAGIC   * streaming is NOT considered because we do not expect new products families to be launched every day

# COMMAND ----------

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

spark.sql(f"use {databaseName}")

# COMMAND ----------

# MAGIC %sql
# MAGIC /* this table has a generated column for product_family_nbr
# MAGIC so for change data, we only need to insert new products found, if any
# MAGIC from teh incoming bronze table
# MAGIC */
# MAGIC INSERT INTO silver_dim_product_family (family)
# MAGIC   SELECT DISTINCT
# MAGIC     family
# MAGIC   FROM bronze_train src
# MAGIC   WHERE src.family NOT IN (SELECT DISTINCT family FROM
# MAGIC   silver_dim_product_family)

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_date
# MAGIC * treat this as a SCD 1 dimension
# MAGIC * we build out the date_base table for all dates till 2100, 12, 31
# MAGIC * once we get newer data on the bronze table, with specific updates, we simply handle it via a SCD 1

# COMMAND ----------

from pyspark.sql.functions import col, dayofmonth, dayofweek, last_day, when, date_format, to_date, lit, expr, abs, datediff
import datetime

#create date range using python
start_date = datetime.date(2012, 1, 1)
end_date = datetime.date(2100, 12, 31)
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

# MAGIC %sql
# MAGIC -- date_dim
# MAGIC MERGE INTO silver_dim_date tgt
# MAGIC USING (
# MAGIC   SELECT DISTINCT --have to dedupe the duplicate holiday entries
# MAGIC     d.*,
# MAGIC     IF(h.date IS NOT NULL AND NOT h.transferred, True, False) AS is_national_holiday,
# MAGIC     IF(h.transferred, True, False) AS is_national_holiday_transferred
# MAGIC   FROM date_base AS d
# MAGIC   LEFT OUTER JOIN bronze_holidays_events AS h ON
# MAGIC     d.date = h.date
# MAGIC     AND (h.date IS NULL or h.locale = 'National')
# MAGIC ORDER BY
# MAGIC   d.date) src
# MAGIC
# MAGIC   ON tgt.date = src.date
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.is_payday = src.is_payday,
# MAGIC     tgt.day_of_week = src.day_of_week,
# MAGIC     tgt.is_weekend = src.is_weekend,
# MAGIC     tgt.is_earthquake_recovery = src.is_earthquake_recovery,
# MAGIC     tgt.days_since_earthquake = src.days_since_earthquake,
# MAGIC     tgt.is_national_holiday = src.is_national_holiday,
# MAGIC     tgt.is_national_holiday_transferred = src.is_national_holiday_transferred
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     `date`,
# MAGIC     is_payday,
# MAGIC     day_of_week,
# MAGIC     is_weekend,
# MAGIC     is_earthquake_recovery,
# MAGIC     days_since_earthquake,
# MAGIC     is_national_holiday,
# MAGIC     is_national_holiday_transferred
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.`date`,
# MAGIC     src.is_payday,
# MAGIC     src.day_of_week,
# MAGIC     src.is_weekend,
# MAGIC     src.is_earthquake_recovery,
# MAGIC     src.days_since_earthquake,
# MAGIC     src.is_national_holiday,
# MAGIC     src.is_national_holiday_transferred
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_dim_date
# MAGIC where is_national_holiday is TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_regional_holiday
# MAGIC * the composite key `date`, `type` and `locale` form a unique combination
# MAGIC * the upsert is made based on that
# MAGIC * treating this as a SCD1, batch mode

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_dim_regional_holiday tgt
# MAGIC USING (
# MAGIC   SELECT DISTINCT
# MAGIC     h.*
# MAGIC   FROM bronze_holidays_events AS h
# MAGIC   WHERE
# MAGIC     h.locale != 'National'
# MAGIC     AND NOT h.transferred
# MAGIC   ORDER BY
# MAGIC     h.date) src
# MAGIC ON tgt.date = src.date
# MAGIC and tgt.type = src.type
# MAGIC and tgt.locale = src.locale
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.description = src.description,
# MAGIC     tgt.transferred = src.transferred
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     `date`,
# MAGIC     `type`,
# MAGIC     locale,
# MAGIC     locale_name,
# MAGIC     description,
# MAGIC     transferred
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.`date`,
# MAGIC     src.`type`,
# MAGIC     src.locale,
# MAGIC     src.locale_name,
# MAGIC     src.description,
# MAGIC     src.transferred
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_store
# MAGIC * truly treat as SDC2 with cdf enabled on target silver_dim_store
# MAGIC * this table sources `bronze_stores` table 
# MAGIC * the process will read a stream from the source table , using changeDataFeed mode
# MAGIC * it would then update the table using merge/update mode

# COMMAND ----------

# create a read stream
read_bronze_dim = spark.readStream.format("delta")\
  .table("bronze_stores")

# COMMAND ----------

# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDelta(microBatchOutputDF, batchId):
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("dim_store_updates")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe

  # In Databricks Runtime 10.5 and below, you must use the following:
  # microBatchOutputDF._jdf.sparkSession().sql("""
  microBatchOutputDF.sparkSession.sql("""
    MERGE INTO silver_dim_stores t
    USING dim_store_updates s
    ON s.store_nbr = t.store_nbr
    AND s.city = t.city
    AND s.state = t.start
    WHEN MATCHED THEN UPDATE SET 
    t.type = s.type,
    t.cluster = s.cluster
    WHEN NOT MATCHED THEN INSERT 
    (store_nbr,
    city,
    `state`,
    `type`,
    cluster)
    VALUES 
    (s.store_nbr,
    s.city,
    s.state,
    s.type,
    s.cluster)

  """)

# Write the output of a streaming aggregation query into Delta table
(read_bronze_dim.writeStream
  .format("delta")
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .option("checkpointLocation","/mnt/g5/silver/checkpointDimStore")
  .start()
)

# COMMAND ----------

read_bronze_dim.writeStream.format("delta").outputMode("append").option("checkpointLocation","/mnt/g5/silver/checkpointDimStore").toTable("silver_dim_store")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_transactions
# MAGIC * also treat this as SCD2

# COMMAND ----------

# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDeltaTransactions(microBatchOutputDF, batchId):
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("dim_transactions_updates")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe

  # In Databricks Runtime 10.5 and below, you must use the following:
  # microBatchOutputDF._jdf.sparkSession().sql("""
  microBatchOutputDF.sparkSession.sql("""
    MERGE INTO silver_dim_transactions t
    USING dim_transactions_updates s
    ON t.date = s.date
    AND t.store_nbr = s.store_nbr
    WHEN MATCHED THEN UPDATE SET 
        t.transactions = s.transactions
        WHEN NOT MATCHED THEN INSERT 
        (`date`,
        store_nbr,
        transactions)
        VALUES 
        (s.date,
        s.store_nbr,
        s.transactions)

  """)

# create a read stream
read_bronze_transaction = spark.readStream.format("delta")\
  .table("bronze_transactions")

# Write the output of a streaming aggregation query into Delta table
(read_bronze_transaction.writeStream
  .format("delta")
  .foreachBatch(upsertToDeltaTransactions)
  .outputMode("update")
  .option("checkpointLocation","/mnt/g5/silver/checkpointDimTransaction")
  .start()
)

# COMMAND ----------


