# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Data Load
# MAGIC * Source data from bronze layers
# MAGIC * for this , we leverage a common layer that was built `01_build_env` notebook
# MAGIC * Also we leverage files for checkpoints and files for silverzone

# COMMAND ----------

# MAGIC %fs ls /mnt/g5

# COMMAND ----------

# MAGIC %fs ls /mnt/g5/bronze/

# COMMAND ----------

# MAGIC %fs ls /mnt/g5/bronze/holiday_events

# COMMAND ----------

# set database name to current user for analysis
import re
userName = spark.sql("SELECT CURRENT_USER").collect()[0]['current_user()']
userName0 = userName.split("@")[0]
userName0 = re.sub('[!#$%&\'*+-/=?^`{}|\.]+', '_', userName0)
databaseName = f"{userName0}_final_Project"

# COMMAND ----------

# MAGIC %python
# MAGIC # Validate if the bronze tables and silver tables are created
# MAGIC # we can get silver_* tables only after completing the notebook, so first time around
# MAGIC # we do not get those tables
# MAGIC display(spark.sql(f"SHOW TABLES FROM {databaseName} LIKE 'bronze_*|silver_*'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate dimensions tables , fact and audit table
# MAGIC * Implemented Change Data Feed on dimensions table
# MAGIC * Implement join queries to populate fact tables

# COMMAND ----------

holiday_table_name = f"{databaseName}.silver_holiday_events"
print (holiday_table_name) 
spark.sql(f"USE {databaseName}")

# COMMAND ----------

# display(spark.sql(f"SELECT * FROM {holiday_table_name} LIMIT 3"))

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {holiday_table_name}");

# COMMAND ----------

# display(spark.sql(f"SELECT * FROM {holiday_table_name} LIMIT 3"))

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {table_name}");
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

try:
  import great_expectations as gx
  from great_expectations.checkpoint import Checkpoint
  print ("Already Installed - Loaded Great Expectations")
except ModuleNotFoundError:
  %pip install great_expectations
  dbutils.library.restartPython()
  import great_expectations as gx
  from great_expectations.checkpoint import Checkpoint
  print ("Loaded Great Expectations after installing")

from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DecimalType, StringType, StructType, StructField, BooleanType
from delta.tables import *
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ### Holiday Events Table from Bronze to Silver
# MAGIC * use streaming data from bronze to silver per architecture
# MAGIC * use change data feed
# MAGIC * test out that new feeds from the bronze table would be streamed into the silver table
# MAGIC * test CDC
# MAGIC * use great_expectation suit to validate incoming data from bronze before streaming into silver

# COMMAND ----------

databaseName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare Great Expectation validation / expectation suite for Holiday Events

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# read parque from bronze holiday parquet
bronze_table = f"{databaseName.lower()}.bronze_holidays_events"
dataframe = spark.sql(f"SELECT * FROM {bronze_table}")

# COMMAND ----------

# setup context first
context_root_dir = "/mnt/g5/silver/great_expectations/"
context = gx.get_context(context_root_dir=context_root_dir)

# COMMAND ----------

# setup datasource next
dataframe_datasource = context.sources.add_or_update_spark(
    name="silver_layer_holiday_events",
)

# COMMAND ----------

# setup data asset
try:
    dataframe_asset = dataframe_datasource.add_dataframe_asset(
        name="silver_holiday_events_df_asset",
        dataframe=dataframe
    )
except ValueError as e:
    print (e)
    pass

# COMMAND ----------

# build the asset
batch_request = dataframe_asset.build_batch_request()

# COMMAND ----------

# setup expectation suite
expectation_suite_name = "silver_holiday_events_expectation_suite"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)
print (validator.head())

# COMMAND ----------

validator.expect_column_values_to_not_be_null(column="type")
type_values = spark.sql(f"select distinct(`type`) from {databaseName}.bronze_holidays_events").rdd.map(lambda row : row[0]).collect()
print(f"Holiday Type should from the following list {type_values}")
test_values_validator = validator.expect_column_distinct_values_to_be_in_set(
    "type", ['Event', 'Holiday', 'Transfer', 'Bridge', 'Additional', "abc"] # on purpose listed "abc", it should be "Work Day"
)
validator.save_expectation_suite(discard_failed_expectations=False)
print (f"Result of Validation Test 01 is {test_values_validator['success']}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare to load Holiday Events in Silver
# MAGIC * enable CDC as `ALTER` script

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

# define a non-stream dataframe reading from the bronze tables
# if we change this to readStream, the Great Expectation does not work out of the box
df_holiday_events = spark.readStream.format("delta")\
    .option("header", "true")\
    .option("changedatafeed", "true")\
    .option("inferSchema", "true")\
    .table(f"{databaseName}.bronze_holidays_events")

# COMMAND ----------

df_holiday_events.isStreaming

# COMMAND ----------



# COMMAND ----------

df_holiday_events.writeStream.option("overwrite","True").saveAsTable(f"{holiday_table_name}")

# COMMAND ----------

display(spark.sql(f"SELECT * from {holiday_table_name} LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Enable CDF for the table

# COMMAND ----------

spark.sql(f"ALTER TABLE {holiday_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed=true)")

# COMMAND ----------

display(spark.sql(f"DESC HISTORY {holiday_table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test CDC effect on Holidays
# MAGIC * add new rows to the bronze table adhoc test the CDC impact

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {databaseName}.bronze_holidays_events LIMIT 3")) 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO anp6911_final_project.bronze_holidays_events (date, type, locale, locale_name, description, transferred) VALUES("2020-01-01","Holiday","National","Manta","Test Manta Holiday Insert", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM anp6911_final_project.bronze_holidays_events where `date` = "2020-01-01"

# COMMAND ----------

bronzeDf = spark.read.format("delta")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .table(f"{databaseName}.bronze_holidays_events")\
    .select(
      "date",
              "type",
              "locale",
              "locale_name",
              "description",
              "transferred")

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Rank the rows for each date based on a priority column (here, we use "transferred")
ranked_df = bronzeDf.withColumn(
    "rank",
    row_number().over(
        Window.partitionBy("date").orderBy("type")
    )
)

# Select only the rows with rank = 1, which correspond to the highest priority row for each date
deduplicated_df = ranked_df.where("rank = 1").drop("rank")
deduplicated_df.createTempView("deduplicated_holidays_events")

# COMMAND ----------

merge_query = f"MERGE INTO {holiday_table_name} as target USING deduplicated_holidays_events  AS source ON target.`date` = source.date WHEN MATCHED THEN UPDATE SET target.`date` = source.date,`type` = source.type, locale = source.locale,locale_name = source.locale_name,`description` = source.description,transferred = source.transferred WHEN NOT MATCHED THEN INSERT (`date`,`type`,locale,locale_name,`description`,transferred) VALUES (source.date, source.type, source.locale, source.locale_name,source.description,source.transferred)"

display(spark.sql(merge_query))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM table_changes({holiday_table_name}, 0, 2) order by _commit_timestamp"))

# COMMAND ----------

display(spark.sql(merge_query))

# COMMAND ----------


