# Databricks notebook source
# MAGIC %md
# MAGIC # Batch and Structured Streaming for Silver dimensions

# COMMAND ----------

spark.sql(f"use fp_g5");

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_oil_prices LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

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
    MERGE INTO silver_dim_oil_prices t
    USING dim_oil_updates s
    ON t.date = s.date
    WHEN MATCHED THEN UPDATE SET 
        t.oil_price = s.dcoilwtico
        WHEN NOT MATCHED THEN INSERT 
        (`date`,
        oil_price)
        VALUES 
        (s.date,
        s.dcoilwtico)

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


