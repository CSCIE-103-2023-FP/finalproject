# Databricks notebook source
spark.sql(f"use fp_g5")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- no need to enable CDF, since we anticipate this to be insert only, but still enabling cdf for consistency across all silver tables
# MAGIC DROP TABLE IF EXISTS silver_dim_product_family;
# MAGIC
# MAGIC CREATE TABLE silver_dim_product_family (product_family_nbr BIGINT GENERATED ALWAYS AS IDENTITY, family VARCHAR(100)) TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC INSERT INTO silver_dim_product_family  (family) 
# MAGIC SELECT DISTINCT
# MAGIC     family
# MAGIC FROM bronze_train
# MAGIC ORDER BY family

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY silver_dim_product_family

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes("silver_dim_product_family",0,10) LIMIT 10

# COMMAND ----------

# DBTITLE 1,Fact table is the training set
# MAGIC %sql
# MAGIC
# MAGIC  USE fp_g5;
# MAGIC -- come back to this fact, does the fact needs CDF?
# MAGIC DROP TABLE IF EXISTS silver_fact_sales;
# MAGIC
# MAGIC CREATE TABLE silver_fact_sales (id BIGINT GENERATED ALWAYS AS IDENTITY, `date` DATE, store_nbr INT, product_family_nbr  INT, sales INT, onpromotion INT) TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC INSERT INTO silver_fact_sales (`date`, store_nbr, product_family_nbr, sales, onpromotion)
# MAGIC SELECT
# MAGIC   `date`,
# MAGIC   store_nbr,
# MAGIC   pf.product_family_nbr,
# MAGIC   sales,
# MAGIC   onpromotion
# MAGIC FROM bronze_train AS bt
# MAGIC INNER JOIN silver_dim_product_family AS pf ON
# MAGIC   bt.family = pf.family

# COMMAND ----------

# MAGIC %sql
# MAGIC USE fp_g5;
# MAGIC
# MAGIC DROP TABLE IF EXISTS silver_dim_store;
# MAGIC
# MAGIC CREATE TABLE silver_dim_store (id BIGINT GENERATED ALWAYS AS IDENTITY, store_nbr INT, city VARCHAR(100), `state` VARCHAR(100), `type` VARCHAR(2), cluster INT) TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC INSERT INTO silver_dim_store (store_nbr, city, `state`, `type`, cluster)
# MAGIC   SELECT
# MAGIC   b.store_nbr,
# MAGIC   b.city,
# MAGIC   b.state,
# MAGIC   b.type,
# MAGIC   b.cluster
# MAGIC   FROM bronze_stores b

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY silver_dim_store

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes("silver_dim_store",0,10) LIMIT 10

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
# MAGIC USE fp_g5;
# MAGIC
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
# MAGIC
# MAGIC USE fp_g5;
# MAGIC
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

# MAGIC %sql
# MAGIC
# MAGIC USE fp_g5;
# MAGIC
# MAGIC DROP TABLE IF EXISTS silver_dim_transactions;
# MAGIC
# MAGIC CREATE TABLE silver_dim_transactions (id BIGINT GENERATED ALWAYS AS IDENTITY, `date` DATE, store_nbr INT, transactions INT) TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_dim_transactions t
# MAGIC USING bronze_transactions s
# MAGIC ON t.date = s.date
# MAGIC AND t.store_nbr = s.store_nbr
# MAGIC WHEN MATCHED THEN UPDATE SET 
# MAGIC     t.transactions = s.transactions
# MAGIC     WHEN NOT MATCHED THEN INSERT 
# MAGIC     (`date`,
# MAGIC     store_nbr,
# MAGIC     transactions)
# MAGIC     VALUES 
# MAGIC     (s.date,
# MAGIC     s.store_nbr,
# MAGIC     s.transactions)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes("silver_dim_transactions",0,1 ) LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_dim_oil_prices
# MAGIC * treat this as cdf, with just the date and oil value
# MAGIC * This is supposed to only contain a row for a date and oil value

# COMMAND ----------

# MAGIC %sql
# MAGIC USE fp_g5;
# MAGIC
# MAGIC DROP TABLE IF EXISTS silver_dim_oil_prices;
# MAGIC
# MAGIC CREATE TABLE silver_dim_oil_prices(id BIGINT GENERATED ALWAYS AS IDENTITY, `date` DATE, oil_price DECIMAL(14,2)) TBLPROPERTIES (delta.enableChangeDataFeed= true)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver_dim_oil_prices (`date`, oil_price) 
# MAGIC SELECT * FROM bronze_oil_prices;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes("silver_dim_oil_prices",0) LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_holidays_events - as is for gold layer
# MAGIC * this does not follow the kimball setup
# MAGIC * this is specific based on gold build requirements

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_holidays_events LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE fp_g5;
# MAGIC
# MAGIC DROP TABLE IF EXISTS silver_dim_holidays_events;
# MAGIC
# MAGIC CREATE TABLE silver_dim_holidays_events (id BIGINT GENERATED BY DEFAULT AS IDENTITY, `date` DATE, `type` VARCHAR(100), locale VARCHAR(100), locale_name VARCHAR(100), `description` VARCHAR(100), transferred BOOLEAN) TBLPROPERTIES (delta.enableChangeDataFeed= true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silver_dim_holidays_events t
# MAGIC USING bronze_holidays_events s
# MAGIC ON
# MAGIC t.date = s.date AND
# MAGIC t.type = s.type AND
# MAGIC t.locale = s.locale AND
# MAGIC t.locale_name = s.locale_name
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC t.description = s.description,
# MAGIC t.transferred = s.transferred
# MAGIC WHEN NOT MATCHED THEN INSERT 
# MAGIC     (`date`,
# MAGIC     `type`,
# MAGIC     `locale`,
# MAGIC     `locale_name`,
# MAGIC     `description`,
# MAGIC     `transferred`)
# MAGIC     VALUES 
# MAGIC     (s.`date`,
# MAGIC     s.`type`,
# MAGIC     s.`locale`,
# MAGIC     s.`locale_name`,
# MAGIC     s.`description`,
# MAGIC     s.`transferred`)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_fact_training

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS fp_g5.silver_fact_train;
# MAGIC
# MAGIC CREATE TABLE fp_g5.silver_fact_train (id INT, `date` DATE, store_nbr INT, family VARCHAR(100), sales INT, onpromotion INT) TBLPROPERTIES (delta.enableChangeDataFeed=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO fp_g5.silver_fact_train t
# MAGIC USING fp_g5.bronze_train s
# MAGIC ON t.date = s.date
# MAGIC AND t.store_nbr = s.store_nbr
# MAGIC AND t.family = s.family
# MAGIC WHEN MATCHED THEN UPDATE SET 
# MAGIC     t.sales = s.sales,
# MAGIC     t.onpromotion = s.onpromotion
# MAGIC     WHEN NOT MATCHED THEN INSERT 
# MAGIC     (id, `date`,
# MAGIC     store_nbr,
# MAGIC     sales, onpromotion)
# MAGIC     VALUES 
# MAGIC     (s.id,
# MAGIC     s.date,
# MAGIC     s.store_nbr,
# MAGIC     s.sales,
# MAGIC     s.onpromotion) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze_train

# COMMAND ----------


