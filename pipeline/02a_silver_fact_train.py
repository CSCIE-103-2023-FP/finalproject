# Databricks notebook source
# MAGIC %md
# MAGIC # Batch and Structured Streaming for Silver dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_fact_train
# MAGIC * daily/ batch approach would be to check if there are any new family of products which are not there
# MAGIC   * if not found insert
# MAGIC   * if found ignore
# MAGIC   * streaming is considered because we do not expect new products families to be launched every day

# COMMAND ----------

spark.sql(f"use fp_g5");

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM anp6911_finalproject_01.bronze_train LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDeltaTransactions(microBatchOutputDF, batchId):
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("fact_train")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe

  # In Databricks Runtime 10.5 and below, you must use the following:
  # microBatchOutputDF._jdf.sparkSession().sql("""
  microBatchOutputDF.sparkSession.sql("""
    MERGE INTO silver_fact_train t
    USING fact_train s
    ON t.date = s.date
    AND t.store_nbr = s.store_nbr
    AND t.family = s.family
    WHEN MATCHED THEN UPDATE SET 
        t.sales = s.sales,
        t.onpromotion = s.onpromotion
        WHEN NOT MATCHED THEN INSERT 
        (id,`date`,store_nbr, family, sales, onpromotion)
        VALUES 
        (s.id,
        s.date,
        s.store_nbr,
        s.family,
        s.sales,
        s.onpromotion)

  """)

# create a read stream
read_bronze_train = spark.readStream.format("delta")\
  .table("bronze_train")

# Write the output of a streaming aggregation query into Delta table
(read_bronze_train.writeStream
  .format("delta")
  .foreachBatch(upsertToDeltaTransactions)
  .outputMode("update")
  .option("checkpointLocation","/mnt/g5/silver/checkpointfactTrain")
  .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_oil
# MAGIC
# MAGIC

# COMMAND ----------

# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDeltaTransactions(microBatchOutputDF, batchId):
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("dim_oil_updates")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe

  # In Databricks Runtime 10.5 and below, you must use the following:
  # microBatchOutputDF._jdf.sparkSession().sql("""
  microBatchOutputDF.sparkSession.sql("""
    MERGE INTO silver_dim_oil t
    USING dim_oil_updates s
    ON t.date = s.date
    WHEN MATCHED THEN UPDATE SET 
        t.oil_price = s.oil_price
        WHEN NOT MATCHED THEN INSERT 
        (`date`,
        oil_price)
        VALUES 
        (s.date,
        s.oil_price)

  """)

# create a read stream
read_bronze_transaction = spark.readStream.format("delta")\
  .table("bronze_oil_prices")

# Write the output of a streaming aggregation query into Delta table
(read_bronze_transaction.writeStream
  .format("delta")
  .foreachBatch(upsertToDeltaTransactions)
  .outputMode("update")
  .option("checkpointLocation","/mnt/g5/silver/checkpointDimOil")
  .start()
)

# COMMAND ----------


