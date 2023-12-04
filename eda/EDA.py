# Databricks notebook source
# MAGIC %fs ls /mnt/data/2023-kaggle-final

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/data/2023-kaggle-final/energy-prediction/

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %fs ls dbfs:/mnt/data/2023-kaggle-final/energy-prediction/example_test_files/
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %fs ls  dbfs:/mnt/data/2023-kaggle-final/store-sales/

# COMMAND ----------

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
# MAGIC select train.*, stores.*, oil.*, hol.*,trans.*
# MAGIC from
# MAGIC (select * from train_set) train
# MAGIC left join 
# MAGIC (select * from stores) stores
# MAGIC on train.store_nbr = stores.store_nbr
# MAGIC left join (select * from oil) oil
# MAGIC on oil.date = train.date
# MAGIC left join (select * from holidays_events) hol
# MAGIC on hol.date = train.date
# MAGIC left join (select * from transactions) trans
# MAGIC on trans.date = train.date
# MAGIC and trans.store_nbr = train.store_nbr
# MAGIC --and city = locale_name

# COMMAND ----------

select train.*, stores.*, oil.*, hol.*,trans.*
from
(select * from train_set) train
left join 
(select * from stores) stores
on train.store_nbr = stores.store_nbr
left join (select * from oil) oil
on oil.date = train.date
left join (select * from holidays_events) hol
on hol.date = train.date
left join (select * from transactions) trans
on trans.date = train.date

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct cluster from stores
# MAGIC where store_nbr = 25
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC #sum of total sales and total transactions per month per store nbr per product family - sales info
# MAGIC # number of products per store nbr - product ingfo
# MAGIC # number of stores per city per cluster
# MAGIC # number of sales and transactions per city per cluster

# COMMAND ----------

StorefilePath = [('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/client.csv', 'client'),
('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/electricity_prices.csv', 'electricity_prices'),
('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/forecast_weather.csv','forecast_weather') ,
('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/gas_prices.csv','gas_prices'),
('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/historical_weather.csv','historical_weather'),
('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/train.csv','train_energy_set'),
('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/weather_station_to_county_mapping.csv','weather_station')]

for file_name, tab_name in StorefilePath:
  EnergyDF = (spark.read
    .option("sep", ",")
    .option("header", True)
    .csv(file_name))
  

  EnergyDF.createOrReplaceTempView(tab_name)

county_DF  = spark.read.json("dbfs:/mnt/data/2023-kaggle-final/energy-prediction/county_id_to_name_map.json") 
EnergyDF.createOrReplaceTempView('county_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select train.*, client.*
# MAGIC from
# MAGIC (select * from train_energy_set) train
# MAGIC left join (select * from client) client
# MAGIC on train.data_block_id = client.data_block_id
# MAGIC and train.county = client.county
# MAGIC and train.product_type = client.product_type

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from client
