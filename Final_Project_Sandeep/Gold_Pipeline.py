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
    .csv('dbfs:/mnt/data/2023-kaggle-final/store-sales/stores.csv'))

  StoresDF.createOrReplaceTempView(tab_name)

# COMMAND ----------

TrainDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv('dbfs:/mnt/data/2023-kaggle-final/store-sales/train.csv'))

TrainDF.write.mode("append").option("mergeSchema", "true").saveAsTable("silver_train_set9")

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
# MAGIC
# MAGIC SELECT *
# MAGIC             from table_changes('silver_train_set9',4)
# MAGIC   
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists GOLD_SALES_SUMMARY2;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE GOLD_SALES_SUMMARY2 (store_nbr string, family STRING, month_train_date string, year_train_date string, total_sales STRING) USING delta 

# COMMAND ----------

# MAGIC %md
# MAGIC --sum of total sales per month per store nbr per product family - sales info
# MAGIC --number of products per store nbr - product ingfo
# MAGIC --number of stores per city per cluster 
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
# MAGIC MERGE INTO GOLD_SALES_SUMMARY2 USING
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
# MAGIC on GOLD_SALES_SUMMARY2.store_nbr = cdf_silver.store_nbr
# MAGIC and GOLD_SALES_SUMMARY2.family = cdf_silver.family
# MAGIC and GOLD_SALES_SUMMARY2.month_train_date = cdf_silver.month_train_date
# MAGIC and GOLD_SALES_SUMMARY2.year_train_date = cdf_silver.year_train_date
# MAGIC when matched and cdf_silver._change_type = 'update_postimage' then
# MAGIC     update set GOLD_SALES_SUMMARY2.total_sales = cdf_silver.total_sales
# MAGIC when not matched then
# MAGIC     insert (store_nbr,family, month_train_date, year_train_date, total_sales) values (store_nbr,family, month_train_date, year_train_date, total_sales)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from GOLD_SALES_SUMMARY2
# MAGIC  
