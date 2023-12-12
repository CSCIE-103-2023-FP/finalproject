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
# MAGIC drop table if exists silver_train_set9;
# MAGIC
# MAGIC CREATE TABLE silver_train_set9 (id string, date string, store_nbr string, family STRING, SALES string, ONPROMOTION string) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists silver_oil_set9;
# MAGIC
# MAGIC CREATE TABLE silver_oil_set9 (date string, dcoilwtico string) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists silver_transactions_set10;
# MAGIC
# MAGIC CREATE TABLE silver_transactions_set10 (date string, store_nbr string, transactions string) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists silver_stores_set12;
# MAGIC
# MAGIC CREATE TABLE silver_stores_set12 (store_nbr string, city string, state string, type string, cluster string) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists silver_holiday_set13;
# MAGIC
# MAGIC CREATE TABLE silver_holiday_set13 (date string, type string, locale string, locale_name string, description string,
# MAGIC transferred string) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

#execute
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
# MAGIC --where store_nbr = '45'
# MAGIC --where month(date) = '2' and year(date) = '2013' and store_nbr = '25'

# COMMAND ----------



TrainDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv('dbfs:/mnt/data/2023-kaggle-final/store-sales/train.csv'))

TrainDF.write.mode("append").option("mergeSchema", "true").saveAsTable("silver_train_set9")

# COMMAND ----------

#execute
OilDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv('dbfs:/mnt/data/2023-kaggle-final/store-sales/oil.csv'))

OilDF.write.mode("append").option("mergeSchema", "true").saveAsTable("silver_oil_set9")

# COMMAND ----------

#execute
OilDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv('dbfs:/mnt/data/2023-kaggle-final/store-sales/transactions.csv'))

OilDF.write.mode("append").option("mergeSchema", "true").saveAsTable("silver_transactions_set10")




# COMMAND ----------

#execute
StorDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv('dbfs:/mnt/data/2023-kaggle-final/store-sales/stores.csv'))

StorDF.write.mode("append").option("mergeSchema", "true").saveAsTable("silver_stores_set12")


# COMMAND ----------

#execute
holDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv('dbfs:/mnt/data/2023-kaggle-final/store-sales/holidays_events.csv'))

holDF.write.mode("append").option("mergeSchema", "true").saveAsTable("silver_holiday_set13")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from silver_train_set9
# MAGIC --where date = '2015-06-06';
# MAGIC
# MAGIC --insert into silver_train_set9 VALUES('1001123', '2016-05-05','1','AUTOMOTIVE','30','0');
# MAGIC --update silver_train_set9
# MAGIC --set sales = 500
# MAGIC --where store_nbr = 1 and id = '1575288';
# MAGIC --select * from table_changes('silver_train_set9');
# MAGIC
# MAGIC --delete from silver_train_set9 where id = '1000010';
# MAGIC
# MAGIC --SELECT *
# MAGIC   --          from table_changes('silver_stores_set11',0)
# MAGIC     --        where date(_commit_timestamp) = '2023-12-07'
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
# MAGIC
# MAGIC --insert into silver_holiday_set13 VALUES('2023-12-07', 'Holiday', 'Local', 'test', 'test', 'False');
# MAGIC
# MAGIC --update silver_holiday_set13
# MAGIC --set description = 'test'
# MAGIC --where date = '2013-07-03';
# MAGIC --select * from table_changes('silver_train_set9');
# MAGIC
# MAGIC --delete from silver_train_set9 where id = '1000010';
# MAGIC
# MAGIC SELECT *
# MAGIC             from table_changes('silver_train_set9',2)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists OIL_PRICE_SUMMARY2;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE OIL_PRICE_SUMMARY2 (MONTH_OIL_PRICE string,YEAR_OIL_PRICE STRING, avg_dcoilwtico STRING) USING delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists OIL_PRICE_SUMMARY2;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE OIL_PRICE_SUMMARY2 (MONTH_OIL_PRICE string,YEAR_OIL_PRICE STRING, avg_dcoilwtico STRING) USING delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists GOLD_SALES_SUMMARY3;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE GOLD_SALES_SUMMARY4 (store_nbr string, family STRING, month_train_date string, year_train_date string, total_sales STRING) USING delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists GOLD_TRANSACTIONS_SUMMARY4;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE GOLD_TRANSACTIONS_SUMMARY4 (store_nbr string, month_transactions_date string, year_transactions_date string, total_transactions STRING) USING delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists GOLD_STORES_SUMMARY7;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE GOLD_STORES_SUMMARY7 (store_nbr string, CITY string, STATE string, TYPE STRING, CLUSTER STRING, active_ind string, expiry_timestmp string, stat string) USING delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC drop table if exists GOLD_HOLIDAY_SUMMARY7;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE GOLD_HOLIDAY_SUMMARY7 (date string, type string, locale string, locale_name STRING, description STRING,transferred string, active_ind string, expiry_timestmp string, stat string) USING delta 

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
# MAGIC --execute
# MAGIC --sum of total sales  per month per store nbr per product family - sales info
# MAGIC MERGE INTO GOLD_SALES_SUMMARY4 USING
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
# MAGIC on GOLD_SALES_SUMMARY4.store_nbr = cdf_silver.store_nbr
# MAGIC and GOLD_SALES_SUMMARY4.family = cdf_silver.family
# MAGIC and GOLD_SALES_SUMMARY4.month_train_date = cdf_silver.month_train_date
# MAGIC and GOLD_SALES_SUMMARY4.year_train_date = cdf_silver.year_train_date
# MAGIC when matched and cdf_silver._change_type = 'update_postimage' then
# MAGIC     update set GOLD_SALES_SUMMARY4.total_sales = cdf_silver.total_sales
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
# MAGIC --execute
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
# MAGIC --execute
# MAGIC --sum of total sales  per month per store nbr per product family - sales info
# MAGIC MERGE INTO GOLD_TRANSACTIONS_SUMMARY4 USING
# MAGIC (select distinct trans.store_nbr, month(trans.date) as month_transactions_date , year(trans.date) AS year_transactions_date , 
# MAGIC     SUM(TRANSACTIONS) OVER(PARTITION BY trans.store_nbr,month(trans.date) , year(trans.date) 
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
# MAGIC select * from GOLD_TRANSACTIONS_SUMMARY4
# MAGIC where store_nbr = '25'

# COMMAND ----------

# MAGIC %sql
# MAGIC select slvr.*, gld.active_ind, gld.expiry_timestmp
# MAGIC from
# MAGIC (SELECT distinct store_nbr, city, state, type, cluster, _change_type
# MAGIC             from table_changes('silver_stores_set11',2)
# MAGIC             WHERE date(_COMMIT_TIMESTAMP) = CURRENT_DATE()) slvr
# MAGIC left join (select * from GOLD_STORES_SUMMARY5) gld
# MAGIC on slvr.store_nbr = gld.store_nbr

# COMMAND ----------

select distinct trans.store_nbr, month(trans.date) as month_transactions_date , year(trans.date) AS year_transactions_date , 
    SUM(TRANSACTIONS) OVER(PARTITION BY trans.store_nbr,month(trans.date) , year(trans.date) 
                                ORDER BY month(trans.date) desc, year(trans.date)  desc
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_transactions, _change_type
from silver_transactions_set10 trans
INNER JOIN (SELECT distinct store_nbr, month(date) as month_transactions_date, year(date) year_transactions_date, _change_type
            from table_changes('silver_transactions_set10',0)
            where _change_type <> 'update_preimage') change_trans
on month(trans.date) = change_trans.month_transactions_date
and year(trans.date) = change_trans.year_transactions_date
and change_trans.store_nbr = trans.store_nbr

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC --sum of total sales  per month per store nbr per product family - sales info
# MAGIC MERGE INTO GOLD_STORES_SUMMARY7 USING
# MAGIC (SELECT distinct store_nbr, city, state, type, cluster, _change_type
# MAGIC             from table_changes('silver_stores_set12',2)
# MAGIC             WHERE date(_COMMIT_TIMESTAMP) = CURRENT_DATE() ) cdf_silver
# MAGIC on  GOLD_STORES_SUMMARY7.store_nbr = cdf_silver.store_nbr
# MAGIC and GOLD_STORES_SUMMARY7.stat = cdf_silver._change_type
# MAGIC when matched and cdf_silver._change_type = 'update_preimage' then
# MAGIC     update set  active_ind = 'N', expiry_timestmp = current_date(), stat = 'inactive'
# MAGIC when not matched then
# MAGIC     insert (store_nbr, city,state, type, cluster, active_ind, expiry_timestmp, stat)  values (cdf_silver.store_nbr, cdf_silver.city,cdf_silver.state, cdf_silver.type, cdf_silver.cluster, 'Y', '9999-12-31', 'update_preimage');
# MAGIC
# MAGIC update GOLD_STORES_SUMMARY7
# MAGIC set active_ind = 'N' , stat = CURRENT_DATE()
# MAGIC WHERE store_nbr IN (SELECT distinct store_nbr 
# MAGIC             from table_changes('silver_stores_set12',2)
# MAGIC             WHERE date(_COMMIT_TIMESTAMP) = CURRENT_DATE()
# MAGIC             AND _change_type = 'delete')

# COMMAND ----------

# MAGIC %sql
# MAGIC update GOLD_STORES_SUMMARY7
# MAGIC set active_ind = 'N', expiry_timestmp = CURRENT_DATE()
# MAGIC WHERE store_nbr IN (SELECT distinct store_nbr 
# MAGIC             from table_changes('silver_stores_set12',2)
# MAGIC             WHERE date(_COMMIT_TIMESTAMP) = CURRENT_DATE()
# MAGIC             AND _change_type = 'delete')

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from GOLD_STORES_SUMMARY7
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --execute
# MAGIC --sum of total sales  per month per store nbr per product family - sales info
# MAGIC MERGE INTO GOLD_HOLIDAY_SUMMARY7 USING
# MAGIC (SELECT distinct date, type, locale, locale_name, description,transferred, _change_type
# MAGIC             from table_changes('silver_holiday_set13',2)
# MAGIC             WHERE date(_COMMIT_TIMESTAMP) = CURRENT_DATE() ) cdf_silver
# MAGIC on  GOLD_HOLIDAY_SUMMARY7.date = cdf_silver.date
# MAGIC and GOLD_HOLIDAY_SUMMARY7.type = cdf_silver.type
# MAGIC and GOLD_HOLIDAY_SUMMARY7.locale = cdf_silver.locale
# MAGIC and GOLD_HOLIDAY_SUMMARY7.locale_name = cdf_silver.locale_name
# MAGIC and GOLD_HOLIDAY_SUMMARY7.stat = cdf_silver._change_type
# MAGIC when matched and cdf_silver._change_type = 'update_preimage' then
# MAGIC     update set  active_ind = 'N', expiry_timestmp = current_date(), stat = 'inactive'
# MAGIC when not matched then
# MAGIC     insert (date, type,locale, locale_name, description,transferred, active_ind, expiry_timestmp, stat)  values (date, type,locale, locale_name, description,transferred, 'Y', '9999-12-31', 'update_preimage');
# MAGIC
# MAGIC update GOLD_HOLIDAY_SUMMARY7
# MAGIC set active_ind = 'N' , stat = CURRENT_DATE()
# MAGIC WHERE date IN (SELECT distinct date 
# MAGIC             from table_changes('silver_holiday_set13',2)
# MAGIC             WHERE date(_COMMIT_TIMESTAMP) = CURRENT_DATE()
# MAGIC             AND _change_type = 'delete')
# MAGIC and  type IN (SELECT distinct type 
# MAGIC             from table_changes('silver_holiday_set13',2)
# MAGIC             WHERE date(_COMMIT_TIMESTAMP) = CURRENT_DATE()
# MAGIC             AND _change_type = 'delete')
# MAGIC and locale IN (SELECT distinct locale 
# MAGIC             from table_changes('silver_holiday_set13',2)
# MAGIC             WHERE date(_COMMIT_TIMESTAMP) = CURRENT_DATE()
# MAGIC             AND _change_type = 'delete')
# MAGIC and locale_name IN (SELECT distinct locale_name 
# MAGIC             from table_changes('silver_holiday_set13',2)
# MAGIC             WHERE date(_COMMIT_TIMESTAMP) = CURRENT_DATE()
# MAGIC             AND _change_type = 'delete')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GOLD_HOLIDAY_SUMMARY7
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --number of products per store nbr
# MAGIC --execute
# MAGIC drop table if exists STORE_PRODUCTS_SUMMARY12;
# MAGIC
# MAGIC CREATE TABLE if not exists STORE_PRODUCTS_SUMMARY12 USING DELTA  as 
# MAGIC select train.*, city, state, type, cluster
# MAGIC from
# MAGIC       (select distinct store_nbr, family 
# MAGIC   from table_changes('silver_train_set9',0)
# MAGIC   where _change_type in ('insert', 'update_postimage')) train
# MAGIC   left join (select * from GOLD_STORES_SUMMARY7
# MAGIC               where active_ind = 'Y')  str
# MAGIC   on train.store_nbr = str.store_nbr

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from STORE_PRODUCTS_SUMMARY12
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from GOLD_STORES_SUMMARY7

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --sum of total sales per month per store nbr per product family - sales info done (GOLD_SALES_SUMMARY4)
# MAGIC --average of oil price per month per year done (OIL_PRICE_SUMMARY2)
# MAGIC --holiday events in gold -------->scd2 done (GOLD_HOLIDAY_SUMMARY7)
# MAGIC --stores in gold ----------->scd2 (number of stores per city per cluster ) done (GOLD_STORES_SUMMARY7)
# MAGIC --number of products per store nbr - product ing - (STORE_PRODUCTS_SUMMARY12)
# MAGIC --number of transactions per store per city per cluster month year - done (GOLD_TRANSACTIONS_SUMMARY4)
# MAGIC
# MAGIC select * from GOLD_HOLIDAY_SUMMARY7
