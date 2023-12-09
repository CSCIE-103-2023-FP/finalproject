# Databricks notebook source
# MAGIC %md
# MAGIC # Batch and Structured Streaming for Silver dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_store
# MAGIC * truly treat as SDC2 with cdf enabled on target silver_dim_store
# MAGIC * this table sources `bronze_stores` table 
# MAGIC * the process will read a stream from the source table , using changeDataFeed mode
# MAGIC * it would then update the table using merge/update mode

# COMMAND ----------

# MAGIC %sql
# MAGIC USE fp_g5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# create a read stream
read_bronze_dim = spark.readStream.format("delta")\
  .table("bronze_stores")

# COMMAND ----------

# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDeltaDimStores(microBatchOutputDF, batchId):
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("dim_store_updates")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe

  # In Databricks Runtime 10.5 and below, you must use the following:
  # microBatchOutputDF._jdf.sparkSession().sql("""
  microBatchOutputDF.sparkSession.sql("""
    MERGE INTO silver_dim_store t
    USING dim_store_updates s
    ON s.store_nbr = t.store_nbr
    AND s.city = t.city
    AND s.state = t.state
    WHEN MATCHED THEN UPDATE SET 
    t.type = s.type,
    t.cluster = s.cluster
    WHEN NOT MATCHED THEN INSERT 
    (store_nbr,
    city,
    `state`,
    `type`,
    cluster)
    VALUES 
    (s.store_nbr,
    s.city,
    s.state,
    s.type,
    s.cluster)

  """)

# Write the output of a streaming aggregation query into Delta table
(read_bronze_dim.writeStream
  .format("delta")
  .foreachBatch(upsertToDeltaDimStores)
  .outputMode("update")
  .option("checkpointLocation","/mnt/g5/silver/checkpointDimStores")
  .start()
)

# COMMAND ----------


