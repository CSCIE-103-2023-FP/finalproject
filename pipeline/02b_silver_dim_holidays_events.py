# Databricks notebook source
# MAGIC %md
# MAGIC # Batch and Structured Streaming for Silver dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_holidays_events
# MAGIC * stream with cdf
# MAGIC * ensure we dedup incoming data 
# MAGIC * it looks like all columns are specific keys

# COMMAND ----------

# MAGIC %sql
# MAGIC USE fp_g5;

# COMMAND ----------


# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDeltaHolidaysEvents(microBatchOutputDF, batchId):
  # Set the dataframe to view name
  microBatchOutputDF.dropDuplicates().createOrReplaceTempView("dim_holiday_events")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe

  # In Databricks Runtime 10.5 and below, you must use the following:
  # microBatchOutputDF._jdf.sparkSession().sql("""
  microBatchOutputDF.sparkSession.sql("""
    MERGE INTO silver_dim_holidays_events t
    USING dim_holiday_events s
    ON
    t.date = s.date AND
    t.type = s.type AND
    t.locale = s.locale AND
    t.locale_name = s.locale_name AND
    t.description = s.description AND
    t.transferred = s.transferred
    WHEN MATCHED THEN UPDATE SET
    t.description = s.description,
    t.transferred = s.transferred
    WHEN NOT MATCHED THEN INSERT 
        (`date`,
        `type`,
        `locale`,
        `locale_name`,
        `description`,
        `transferred`)
        VALUES 
        (s.`date`,
        s.`type`,
        s.`locale`,
        s.`locale_name`,
        s.`description`,
        s.`transferred`)

  """)

read_bronze_holidays_events = spark.readStream.format("delta").table("bronze_holidays_events")

# Write the output of a streaming aggregation query into Delta table
(read_bronze_holidays_events.writeStream
  .format("delta")
  .foreachBatch(upsertToDeltaHolidaysEvents)
  .outputMode("update")
  .option("checkpointLocation","/mnt/g5/silver/checkpointDimHolidaysEvents")
  .start()
)

