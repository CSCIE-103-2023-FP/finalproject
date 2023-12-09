# Databricks notebook source
# MAGIC %md
# MAGIC # Batch and Structured Streaming for Silver dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_transactions
# MAGIC * also treat this as SCD2

# COMMAND ----------

# MAGIC %sql
# MAGIC USE fp_g5;

# COMMAND ----------

# create a read stream
read_bronze_trans = spark.readStream.format("delta")\
  .table("bronze_transactions")

# COMMAND ----------

# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDelta(microBatchOutputDF, batchId):
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("dim_transactions")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe

  # In Databricks Runtime 10.5 and below, you must use the following:
  # microBatchOutputDF._jdf.sparkSession().sql("""
  microBatchOutputDF.sparkSession.sql("""
    MERGE INTO silver_dim_transactions t
    USING dim_transactions s
    ON s.store_nbr = t.store_nbr
    AND s.date = t.date
    WHEN MATCHED THEN UPDATE SET 
    t.transactions = s.transactions
    WHEN NOT MATCHED THEN INSERT 
    (store_nbr,
    transactions)
    VALUES 
    (s.store_nbr,
    s.transactions)

  """)

# Write the output of a streaming aggregation query into Delta table
(read_bronze_trans.writeStream
  .format("delta")
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .option("checkpointLocation","/mnt/g5/silver/checkpointDimTrans")
  .start()
)

# COMMAND ----------


