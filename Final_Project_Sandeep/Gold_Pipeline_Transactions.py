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
# MAGIC drop table if exists silver_transactions_stage;
# MAGIC
# MAGIC CREATE TABLE silver_transactions_stage (date string, store_nbr string, transactions string) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC
# MAGIC

# COMMAND ----------

#execute
OilDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv('dbfs:/mnt/data/2023-kaggle-final/store-sales/transactions.csv'))

OilDF.write.mode("append").option("mergeSchema", "true").saveAsTable("silver_transactions_stage")




# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists GOLD_TRANSACTIONS_STAGE;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE GOLD_TRANSACTIONS_STAGE (store_nbr string, month_transactions_date string, year_transactions_date string, total_transactions STRING) USING delta 

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
# MAGIC MERGE INTO GOLD_TRANSACTIONS_STAGE USING
# MAGIC (select distinct trans.store_nbr, month(trans.date) as month_transactions_date , year(trans.date) AS year_transactions_date , 
# MAGIC     SUM(TRANSACTIONS) OVER(PARTITION BY trans.store_nbr,month(trans.date) , year(trans.date) 
# MAGIC                                 ORDER BY month(trans.date) desc, year(trans.date)  desc
# MAGIC                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_transactions, _change_type
# MAGIC from silver_transactions_set10 trans
# MAGIC INNER JOIN (SELECT distinct store_nbr, month(date) as month_transactions_date, year(date) year_transactions_date, _change_type
# MAGIC             from table_changes('silver_transactions_stage',0)
# MAGIC             where _change_type <> 'update_preimage') change_trans
# MAGIC on month(trans.date) = change_trans.month_transactions_date
# MAGIC and year(trans.date) = change_trans.year_transactions_date
# MAGIC and change_trans.store_nbr = trans.store_nbr) cdf_silver
# MAGIC on GOLD_TRANSACTIONS_STAGE.month_transactions_date = cdf_silver.month_transactions_date
# MAGIC and GOLD_TRANSACTIONS_STAGE.year_transactions_date = cdf_silver.year_transactions_date
# MAGIC AND GOLD_TRANSACTIONS_STAGE.store_nbr = cdf_silver.store_nbr
# MAGIC when matched and cdf_silver._change_type = 'update_postimage' then
# MAGIC     update set GOLD_TRANSACTIONS_STAGE.total_transactions  = cdf_silver.total_transactions 
# MAGIC when matched and cdf_silver._change_type = 'delete' then
# MAGIC     delete 
# MAGIC when not matched then
# MAGIC     insert (store_nbr, month_transactions_date,year_transactions_date, total_transactions) values (store_nbr, month_transactions_date,year_transactions_date, total_transactions)
