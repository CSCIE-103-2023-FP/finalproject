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
# MAGIC
# MAGIC drop table if exists silver_train_set9;
# MAGIC
# MAGIC CREATE TABLE silver_train_set9 (id string, date string, store_nbr string, family STRING, SALES string, ONPROMOTION string) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists silver_oil_set9;
# MAGIC
# MAGIC CREATE TABLE silver_oil_set9 (date string, dcoilwtico string) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists silver_transactions_set10;
# MAGIC
# MAGIC CREATE TABLE silver_transactions_set10 (date string, store_nbr string, transactions string) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC
# MAGIC

# COMMAND ----------

StorefilePath = [('dbfs:/mnt/data/2023-kaggle-final/store-sales/holidays_events.csv', 'holidays_events'),
('dbfs:/mnt/data/2023-kaggle-final/store-sales/oil.csv', 'oil'),
('dbfs:/mnt/data/2023-kaggle-final/store-sales/sample_submission.csv','sample_submission') ,
('dbfs:/mnt/data/2023-kaggle-final/store-sales/stores.csv','stores'),
('dbfs:/mnt/data/2023-kaggle-final/store-sales/test.csv','test_set'),
('dbfs:/mnt/data/2023-kaggle-final/store-sales/train.csv','train_set'),
('dbfs:/mnt/data/2023-kaggle-final/store-sales/transactions.csv','transactions')]

for file_name, tab_name in StorefilePath:
  StoresDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv(file_name))

  StoresDF.createOrReplaceTempView(tab_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from transactions
# MAGIC where month(date) = '2' and year(date) = '2013' and store_nbr = '25'

# COMMAND ----------

TrainDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv('dbfs:/mnt/data/2023-kaggle-final/store-sales/train.csv'))

TrainDF.write.mode("append").option("mergeSchema", "true").saveAsTable("silver_train_set9")

# COMMAND ----------

OilDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv('dbfs:/mnt/data/2023-kaggle-final/store-sales/oil.csv'))

OilDF.write.mode("append").option("mergeSchema", "true").saveAsTable("silver_oil_set9")

# COMMAND ----------

OilDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv('dbfs:/mnt/data/2023-kaggle-final/store-sales/transactions.csv'))

OilDF.write.mode("append").option("mergeSchema", "true").saveAsTable("silver_transactions_set10")




# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --select * from silver_train_set9
# MAGIC --where date = '2015-06-06';
# MAGIC
# MAGIC --insert into silver_train_set9 VALUES('1000023', '2016-05-05','1','AUTOMOTIVE','30','0');
# MAGIC --update silver_train_set9
# MAGIC --set sales = 500
# MAGIC --where store_nbr = 1 and id = '1575288';
# MAGIC --select * from table_changes('silver_train_set9');
# MAGIC
# MAGIC delete from silver_train_set9 where id = '1000010';
# MAGIC
# MAGIC SELECT *
# MAGIC             from table_changes('silver_train_set9',4)
# MAGIC   
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --select * from silver_train_set9
# MAGIC --where date = '2015-06-06';
# MAGIC
# MAGIC insert into silver_oil_set9 VALUES('2024-01-01', '400');
# MAGIC --update silver_oil_set9
# MAGIC --set dcoilwtico = 500
# MAGIC --where date = '2013-01-01';
# MAGIC --select * from table_changes('silver_train_set9');
# MAGIC
# MAGIC --delete from silver_train_set9 where id = '1000010';
# MAGIC
# MAGIC SELECT *
# MAGIC             from table_changes('silver_oil_set9',2)
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC --insert into silver_transactions_set9 VALUES('1', '2024-01-01', '400');
# MAGIC update silver_transactions_set9
# MAGIC set transactions = 1500
# MAGIC where date = '2013-01-01' and store_nbr = '25';
# MAGIC --select * from table_changes('silver_train_set9');
# MAGIC
# MAGIC --delete from silver_train_set9 where id = '1000010';
# MAGIC
# MAGIC SELECT *
# MAGIC             from table_changes('silver_transactions_set9',2)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists OIL_PRICE_SUMMARY2;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE OIL_PRICE_SUMMARY2 (MONTH_OIL_PRICE string,YEAR_OIL_PRICE STRING, avg_dcoilwtico STRING) USING delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists GOLD_SALES_SUMMARY3;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE GOLD_SALES_SUMMARY3 (store_nbr string, family STRING, month_train_date string, year_train_date string, total_sales STRING) USING delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists GOLD_TRANSACTIONS_SUMMARY4;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE GOLD_TRANSACTIONS_SUMMARY4 (store_nbr string, month_transactions_date string, year_transactions_date string, total_transactions STRING) USING delta 

# COMMAND ----------

# MAGIC %md
# MAGIC --sum of total sales per month per store nbr per product family - sales info
# MAGIC --average of oil price per month per year
# MAGIC --holiday events in gold -------->scd2
# MAGIC --stores in gold ----------->scd2 (number of stores per city per cluster )
# MAGIC --number of products per store nbr - product ingfo
# MAGIC --number of transactions per store per city per cluster month year

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  train.store_nbr, train.family, month(train.date) as month_train_date, year(train.date) year_train_date, SALES, ID,
# MAGIC   sum(sales) OVER(PARTITION BY train.store_nbr,train.family, month(train.date), year(train.date)
# MAGIC                                 ORDER BY month(train.date) desc, year(train.date) desc
# MAGIC                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_sales
# MAGIC from silver_train_set9 train
# MAGIC where store_nbr = 1 and family = 'AUTOMOTIVE' AND MONTH(train.date) = 6 AND YEAR(train.date) = '2015'
# MAGIC --AND ID = '1575288'

# COMMAND ----------

# MAGIC %sql
# MAGIC select train.*
# MAGIC from
# MAGIC (
# MAGIC   select distinct train.store_nbr, train.family, month(train.date) as month_train_date, year(train.date) year_train_date, 
# MAGIC     sum(sales) OVER(PARTITION BY train.store_nbr,train.family, month(train.date), year(train.date)
# MAGIC                                 ORDER BY month(train.date) desc, year(train.date) desc
# MAGIC                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_sales
# MAGIC from silver_train_set9 train
# MAGIC --where store_nbr = 1 and family = 'AUTOMOTIVE' AND MONTH(train.date) = 6 AND YEAR(train.date) = '2015'
# MAGIC ) train
# MAGIC INNER JOIN (SELECT distinct STORE_NBR, family , month(date) as month_change_train_date, year(date) year_change_train_date,  _change_type
# MAGIC             from table_changes('silver_train_set9',4)) as change_train
# MAGIC on train.store_nbr = change_train.store_nbr
# MAGIC and train.family = change_train.family
# MAGIC and month_train_date = change_train.month_change_train_date
# MAGIC and year_train_date = change_train.year_change_train_date

# COMMAND ----------

# MAGIC %sql
# MAGIC --sum of total sales  per month per store nbr per product family - sales info
# MAGIC MERGE INTO GOLD_SALES_SUMMARY3 USING
# MAGIC (select distinct train.store_nbr AS STORE_NBR, train.family AS FAMILY, month(train.date) as month_train_date, year(train.date) year_train_date, 
# MAGIC     sum(sales) OVER(PARTITION BY train.store_nbr,train.family, month(train.date), year(train.date)
# MAGIC                                 ORDER BY month(train.date) desc, year(train.date) desc
# MAGIC                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_sales, _change_type
# MAGIC from silver_train_set9 train
# MAGIC INNER JOIN (SELECT distinct STORE_NBR, family , month(date) as month_change_train_date, year(date) year_change_train_date,
# MAGIC             _change_type
# MAGIC             from table_changes('silver_train_set9',2)
# MAGIC             where _change_type <> 'update_preimage') as change_train
# MAGIC on train.store_nbr = change_train.store_nbr
# MAGIC and train.family = change_train.family
# MAGIC and month(train.date) = change_train.month_change_train_date
# MAGIC and year(train.date) = change_train.year_change_train_date
# MAGIC ) cdf_silver
# MAGIC on GOLD_SALES_SUMMARY3.store_nbr = cdf_silver.store_nbr
# MAGIC and GOLD_SALES_SUMMARY3.family = cdf_silver.family
# MAGIC and GOLD_SALES_SUMMARY3.month_train_date = cdf_silver.month_train_date
# MAGIC and GOLD_SALES_SUMMARY3.year_train_date = cdf_silver.year_train_date
# MAGIC when matched and cdf_silver._change_type = 'update_postimage' then
# MAGIC     update set GOLD_SALES_SUMMARY3.total_sales = cdf_silver.total_sales
# MAGIC when matched and cdf_silver._change_type = 'delete' then
# MAGIC     delete 
# MAGIC when not matched then
# MAGIC     insert (store_nbr,family, month_train_date, year_train_date, total_sales) values (store_nbr,family, month_train_date, year_train_date, total_sales)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from GOLD_SALES_SUMMARY3
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC --sum of total sales  per month per store nbr per product family - sales info
# MAGIC MERGE INTO OIL_PRICE_SUMMARY2 USING
# MAGIC (select distinct month(oil.date) as MONTH_OIL_PRICE , year(oil.date) YEAR_OIL_PRICE , 
# MAGIC     avg(dcoilwtico) OVER(PARTITION BY month(oil.date) , year(oil.date) 
# MAGIC                                 ORDER BY month(oil.date) desc, year(oil.date)  desc
# MAGIC                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS avg_dcoilwtico, _change_type
# MAGIC from silver_oil_set9 oil
# MAGIC INNER JOIN (SELECT distinct month(date) as month_oil_date, year(date) year_oil_date, _change_type
# MAGIC             from table_changes('silver_oil_set9',2)
# MAGIC             where _change_type <> 'update_preimage') change_oil
# MAGIC on month(oil.date) = change_oil.month_oil_date
# MAGIC and year(oil.date) = change_oil.year_oil_date) cdf_silver
# MAGIC on OIL_PRICE_SUMMARY2.MONTH_OIL_PRICE = cdf_silver.MONTH_OIL_PRICE
# MAGIC and OIL_PRICE_SUMMARY2.YEAR_OIL_PRICE = cdf_silver.YEAR_OIL_PRICE
# MAGIC when matched and cdf_silver._change_type = 'update_postimage' then
# MAGIC     update set OIL_PRICE_SUMMARY2.avg_dcoilwtico  = cdf_silver.avg_dcoilwtico 
# MAGIC when matched and cdf_silver._change_type = 'delete' then
# MAGIC     delete 
# MAGIC when not matched then
# MAGIC     insert (MONTH_OIL_PRICE,YEAR_OIL_PRICE, avg_dcoilwtico) values (MONTH_OIL_PRICE,YEAR_OIL_PRICE, avg_dcoilwtico)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from OIL_PRICE_SUMMARY2

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC --sum of total sales  per month per store nbr per product family - sales info
# MAGIC MERGE INTO GOLD_TRANSACTIONS_SUMMARY4 USING
# MAGIC (select distinct trans.store_nbr, month(trans.date) as month_transactions_date , year(trans.date) AS year_transactions_date , 
# MAGIC     SUM(TRANSACTIONS) OVER(PARTITION BY month(trans.date) , year(trans.date) 
# MAGIC                                 ORDER BY month(trans.date) desc, year(trans.date)  desc
# MAGIC                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_transactions, _change_type
# MAGIC from silver_transactions_set10 trans
# MAGIC INNER JOIN (SELECT distinct store_nbr, month(date) as month_transactions_date, year(date) year_transactions_date, _change_type
# MAGIC             from table_changes('silver_transactions_set10',0)
# MAGIC             where _change_type <> 'update_preimage') change_trans
# MAGIC on month(trans.date) = change_trans.month_transactions_date
# MAGIC and year(trans.date) = change_trans.year_transactions_date
# MAGIC and change_trans.store_nbr = trans.store_nbr) cdf_silver
# MAGIC on GOLD_TRANSACTIONS_SUMMARY4.month_transactions_date = cdf_silver.month_transactions_date
# MAGIC and GOLD_TRANSACTIONS_SUMMARY4.year_transactions_date = cdf_silver.year_transactions_date
# MAGIC AND GOLD_TRANSACTIONS_SUMMARY4.store_nbr = cdf_silver.store_nbr
# MAGIC when matched and cdf_silver._change_type = 'update_postimage' then
# MAGIC     update set GOLD_TRANSACTIONS_SUMMARY4.total_transactions  = cdf_silver.total_transactions 
# MAGIC when matched and cdf_silver._change_type = 'delete' then
# MAGIC     delete 
# MAGIC when not matched then
# MAGIC     insert (store_nbr, month_transactions_date,year_transactions_date, total_transactions) values (store_nbr, month_transactions_date,year_transactions_date, total_transactions)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from GOLD_TRANSACTIONS_SUMMARY3
# MAGIC where store_nbr = '25'
