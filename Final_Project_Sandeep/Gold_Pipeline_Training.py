# Databricks notebook source
import re
userName = spark.sql("SELECT CURRENT_USER").collect()[0]['current_user()']
userName0 = userName.split("@")[0]
userName0 = re.sub('[!#$%&\'*+-/=?^`{}|\.]+', '_', userName0)
userName1 = userName.split("@")[1]
userName = f'{userName0}@{userName1}'
dbutils.fs.mkdirs(f"/Users/{userName}/data")
userDir = f"/Users/{userName}/data"
databaseName = f"{userName0}_Assgn_01"

print('databaseName ' + databaseName)
print('UserDir ' + userDir)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")
spark.sql(f"use {databaseName}")

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists silver_train_set_stage;
# MAGIC
# MAGIC CREATE TABLE silver_train_set_stage (id string, date string, store_nbr string, family STRING, SALES string, ONPROMOTION string) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------



TrainDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv('dbfs:/mnt/data/2023-kaggle-final/store-sales/train.csv'))

TrainDF.write.mode("append").option("mergeSchema", "true").saveAsTable("silver_train_set_stage")

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists GOLD_SALES_STAGE;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE GOLD_SALES_STAGE (store_nbr string,family STRING, month_train_date STRING,
# MAGIC                       year_train_date string, total_sales string ) USING delta 

# COMMAND ----------

# MAGIC %md
# MAGIC --sum of total sales per month per store nbr per product family - sales info done (GOLD_SALES_SUMMARY4)
# MAGIC --average of oil price per month per year done (OIL_PRICE_SUMMARY2)
# MAGIC --holiday events in gold -------->scd2 done (GOLD_HOLIDAY_SUMMARY7)
# MAGIC --stores in gold ----------->scd2 (number of stores per city per cluster ) done (GOLD_STORES_SUMMARY7)
# MAGIC --number of products per store nbr - product ing - tbc
# MAGIC --number of transactions per store per city per cluster month year - done (GOLD_TRANSACTIONS_SUMMARY4)

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC --sum of total sales  per month per store nbr per product family - sales info
# MAGIC MERGE INTO GOLD_SALES_STAGE USING
# MAGIC (select distinct train.store_nbr AS STORE_NBR, train.family AS FAMILY, month(train.date) as month_train_date, year(train.date) year_train_date, 
# MAGIC     sum(sales) OVER(PARTITION BY train.store_nbr,train.family, month(train.date), year(train.date)
# MAGIC                                 ORDER BY month(train.date) desc, year(train.date) desc
# MAGIC                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_sales, _change_type
# MAGIC from silver_train_set9 train
# MAGIC INNER JOIN (SELECT distinct STORE_NBR, family , month(date) as month_change_train_date, year(date) year_change_train_date,
# MAGIC             _change_type
# MAGIC             from table_changes('silver_train_set_stage',0)
# MAGIC             where _change_type <> 'update_preimage') as change_train
# MAGIC on train.store_nbr = change_train.store_nbr
# MAGIC and train.family = change_train.family
# MAGIC and month(train.date) = change_train.month_change_train_date
# MAGIC and year(train.date) = change_train.year_change_train_date
# MAGIC ) cdf_silver
# MAGIC on GOLD_SALES_STAGE.store_nbr = cdf_silver.store_nbr
# MAGIC and GOLD_SALES_STAGE.family = cdf_silver.family
# MAGIC and GOLD_SALES_STAGE.month_train_date = cdf_silver.month_train_date
# MAGIC and GOLD_SALES_STAGE.year_train_date = cdf_silver.year_train_date
# MAGIC when matched and cdf_silver._change_type = 'update_postimage' then
# MAGIC     update set GOLD_SALES_STAGE.total_sales = cdf_silver.total_sales
# MAGIC when matched and cdf_silver._change_type = 'delete' then
# MAGIC     delete 
# MAGIC when not matched then
# MAGIC     insert (store_nbr,family, month_train_date, year_train_date, total_sales) values (store_nbr,family, month_train_date, year_train_date, total_sales)
