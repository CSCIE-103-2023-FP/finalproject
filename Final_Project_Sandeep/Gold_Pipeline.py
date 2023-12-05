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
# MAGIC select * from silver_train_set9
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists GOLD_SALES_SUMMARY;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE GOLD_SALES_SUMMARY (store_nbr string, store_city string, store_state string, family STRING, month_train_date string, year_train_date string, total_sales STRING) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC --sum of total sales per month per store nbr per product family - sales info
# MAGIC --number of products per store nbr - product ingfo
# MAGIC --number of stores per city per cluster 
# MAGIC --number of transactions per store per city per cluster month year

# COMMAND ----------

# MAGIC %sql
# MAGIC --sum of total sales  per month per store nbr per product family - sales info
# MAGIC MERGE INTO GOLD_SALES_SUMMARY USING
# MAGIC (select distinct store_nbr,store_city, store_state, family,month(train_date) as month_train_date, year(train_date) year_train_date, sum(sales) OVER(PARTITION BY store_nbr,family, month(train_date), year(train_date)
# MAGIC                                 ORDER BY month(silver_sales.train_date) desc, year(silver_sales.train_date) desc
# MAGIC                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_sales
# MAGIC from
# MAGIC (select train.store_nbr,stores.city as store_city, stores.state as store_state, train.family, train.sales, train.date as train_date
# MAGIC from
# MAGIC (select * from train_set) train
# MAGIC left join 
# MAGIC (select * from stores) stores
# MAGIC on train.store_nbr = stores.store_nbr) silver_sales
# MAGIC INNER JOIN (SELECT STORE_NBR, SALES from table_changes('silver_train_set9',0)) as silver_train
# MAGIC on silver_sales.store_nbr = silver_train.store_nbr) cdf_silver
# MAGIC on GOLD_SALES_SUMMARY.STORE_NBR = cdr_silver.STORE_NBR
# MAGIC when matched then
# MAGIC     update set GOLD_SALES_SUMMARY.total_sales = cdf_silver.total_sales
# MAGIC when not matched then
# MAGIC     insert (store_nbr, store_city, store_state, month_train_date, year_train_date, total_sales) values (store_nbr, store_city, store_state, month_train_date, year_train_date, total_sales)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from train_set
