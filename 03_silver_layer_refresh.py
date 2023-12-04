# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Data Load
# MAGIC * Source data from bronze layers
# MAGIC * for this , we leverage a common layer that was built `01_build_env` notebook

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/g5/silver/checkpoints")

# COMMAND ----------

# set database name
import re
userName = spark.sql("SELECT CURRENT_USER").collect()[0]['current_user()']
userName0 = userName.split("@")[0]
userName0 = re.sub('[!#$%&\'*+-/=?^`{}|\.]+', '_', userName0)
databaseName = f"{userName0}_Final_Project"

# COMMAND ----------

# MAGIC %python
# MAGIC # Validate if the bronze tables are created
# MAGIC display(spark.sql(f"SHOW TABLES FROM {databaseName} LIKE 'bronze_*'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate dimensions tables , fact and audit table
# MAGIC * Implemented Change Data Feed on dimensions table
# MAGIC * Implement join queries to populate fact tables

# COMMAND ----------

# Create holidays_events_dim
from pyspark.sql.types import IntegerType, DecimalType, StringType, StructType, StructField, BooleanType

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM anp6911_final_project.bronze_holidays_events LIMIT 3

# COMMAND ----------

table_name = f"{databaseName}.silver_holiday_events"
print (table_name) 
spark.sql(f"USE {databaseName}")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {databaseName}.silver_holidays_events");

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {table_name}");
# spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} \
#   (`date` DATE, \
#   `type` varchar(20),\
#   locale varchar(10),\
#   locale_name varchar(50),\
#   transferred BOOLEAN,\
#   `description` varchar(100)) \
#   partitioned by (locale)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Dependencies
# MAGIC * GreatExpectations -  More information [here](https://docs.greatexpectations.io/docs/0.15.50/deployment_patterns/how_to_use_great_expectations_in_databricks/)
# MAGIC * pyspark imports as required
# MAGIC * All imports are here

# COMMAND ----------

# MAGIC %pip install great_expectations

# COMMAND ----------

dbutils.library.restartPython() # to ensure the installs are enabled

# COMMAND ----------

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from pyspark.sql.functions import *

# COMMAND ----------

context_root_dir = "/dbfs/great_expectations/"
context = gx.get_context(context_root_dir=context_root_dir)

# COMMAND ----------

df_holiday_events = spark.read.format("delta")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .table(f"{databaseName}.bronze_holidays_events")

# COMMAND ----------

df_holiday_events.display()

# COMMAND ----------

dataframe_datasource = context.sources.add_or_update_spark(
    name="silver_layer_holiday_events",
)

# COMMAND ----------

dataframe_asset = dataframe_datasource.add_dataframe_asset(
    name="silver_holiday_events_df_asset",
    dataframe=df_holiday_events,
)

# COMMAND ----------

batch_request = dataframe_asset.build_batch_request()

# COMMAND ----------

expectation_suite_name = "silver_holiday_events_expectation_suite"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)
print(validator.head())

# COMMAND ----------

validator.expect_column_values_to_not_be_null(column="type")
type_values = spark.sql(f"select distinct(`type`) from {databaseName}.bronze_holidays_events").rdd.map(lambda row : row[0]).collect()
print(f"Holiday Type should from the following list {type_values}")
test_values_validator = validator.expect_column_distinct_values_to_be_in_set(
    "type", ['Event', 'Holiday', 'Transfer', 'Bridge', 'Additional', "abc"] #on purpose listed "abc", it should be "Work Day"
)
validator.save_expectation_suite(discard_failed_expectations=False)
print (f"Result of Validation Test 01 is {test_values_validator['success']}")

# COMMAND ----------

holiday_dim_schema = StructType().add(
  StructField("holidayId", IntegerType())).add(
  StructField("date", StringType())).add(
  StructField("type", StringType())).add(
  StructField("locale", StringType())).add(
  StructField("locale_name", StringType())).add(
  StructField("description", StringType())).add(
  StructField("transferred", StringType())
  )

# COMMAND ----------

# Initial Load
# df =  spark.read.format("delta") \
# .option("startingVersion", 0) \
# .option("inferSchema", False)\
# .table(f"{databaseName}.bronze_holidays_events")
# # df1 = df.withColumn("holidayId", monotonically_increasing_id())

# COMMAND ----------

df_holiday_events.isStreaming

# COMMAND ----------

df_holiday_events.show(5)

# COMMAND ----------

df_holiday_events.write.option("overwrite","True").saveAsTable(table_name)

# COMMAND ----------

display(spark.sql(f"SELECT * from {table_name} LIMIT 10"))

# COMMAND ----------

table_name

# COMMAND ----------

spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed=true)")

# COMMAND ----------

display(spark.sql(f"DESC HISTORY {table_name}"))

# COMMAND ----------

# holiday_dim_schema = StructType().add(
#   StructField("holidayId", IntegerType())).add(
#   StructField("date", StringType())).add(
#   StructField("type", StringType())).add(
#   StructField("locale", StringType())).add(
#   StructField("locale_name", StringType())).add(
#   StructField("description", StringType())).add(
#   StructField("transferred", BooleanType())
#   )

# COMMAND ----------

# tableCreated = spark.catalog.createTable("silver_holiday_events_dim", schema=holiday_dim_schema)

# COMMAND ----------

tableCreated

# COMMAND ----------


