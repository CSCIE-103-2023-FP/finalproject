# Databricks notebook source
# MAGIC %md
# MAGIC # Batch and Structured Streaming for Silver dimensions

# COMMAND ----------

# MAGIC %sql
# MAGIC USE fp_g5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_date
# MAGIC * treat this as a SCD 1 dimension
# MAGIC * we build out the date_base table for all dates till 2100, 12, 31
# MAGIC * once we get newer data on the bronze table, with specific updates, we simply handle it via a SCD 1

# COMMAND ----------

from pyspark.sql.functions import col, dayofmonth, dayofweek, last_day, when, date_format, to_date, lit, expr, abs, datediff
import datetime

#create date range using python
start_date = datetime.date(2012, 1, 1)
end_date = datetime.date(2100, 12, 31)
date_range = [start_date + datetime.timedelta(days=x) for x in range((end_date - start_date).days + 1)]
df_dates = spark.createDataFrame(date_range, "date")

#add additional simple columns based on calendar only
df_dates = (
  df_dates
    .withColumnRenamed("value", "date")
    .withColumn("is_payday", when((dayofmonth(col("date")) == 15) | (col("date") == last_day(col("date"))), True).otherwise(False))
    .withColumn("day_of_week", date_format(col("date"), "EEEE"))
    .withColumn("is_weekend", when(dayofweek(col("date")).isin(6, 7), True).otherwise(False))
)

#add adjustment for the earthquake on 4/16/2016
earthquake_date = to_date(lit("2016-04-16"), "yyyy-MM-dd")
df_dates = (
  df_dates
    .withColumn("is_earthquake_recovery", when((col("date") >= earthquake_date) & (col("date") <= earthquake_date + expr("interval 50 days")), True).otherwise(False))
    .withColumn("days_since_earthquake", when(col("is_earthquake_recovery"), abs(datediff(col("date"), earthquake_date))).otherwise(None))
)

#create a SQL view of this new dataframe
df_dates.createOrReplaceTempView("date_base")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- date_dim
# MAGIC MERGE INTO silver_dim_date tgt
# MAGIC USING (
# MAGIC   SELECT DISTINCT --have to dedupe the duplicate holiday entries
# MAGIC     d.*,
# MAGIC     IF(h.date IS NOT NULL AND NOT h.transferred, True, False) AS is_national_holiday,
# MAGIC     IF(h.transferred, True, False) AS is_national_holiday_transferred
# MAGIC   FROM date_base AS d
# MAGIC   LEFT OUTER JOIN bronze_holidays_events AS h ON
# MAGIC     d.date = h.date
# MAGIC     AND (h.date IS NULL or h.locale = 'National')
# MAGIC ORDER BY
# MAGIC   d.date) src
# MAGIC
# MAGIC   ON tgt.date = src.date
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.is_payday = src.is_payday,
# MAGIC     tgt.day_of_week = src.day_of_week,
# MAGIC     tgt.is_weekend = src.is_weekend,
# MAGIC     tgt.is_earthquake_recovery = src.is_earthquake_recovery,
# MAGIC     tgt.days_since_earthquake = src.days_since_earthquake,
# MAGIC     tgt.is_national_holiday = src.is_national_holiday,
# MAGIC     tgt.is_national_holiday_transferred = src.is_national_holiday_transferred
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     `date`,
# MAGIC     is_payday,
# MAGIC     day_of_week,
# MAGIC     is_weekend,
# MAGIC     is_earthquake_recovery,
# MAGIC     days_since_earthquake,
# MAGIC     is_national_holiday,
# MAGIC     is_national_holiday_transferred
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.`date`,
# MAGIC     src.is_payday,
# MAGIC     src.day_of_week,
# MAGIC     src.is_weekend,
# MAGIC     src.is_earthquake_recovery,
# MAGIC     src.days_since_earthquake,
# MAGIC     src.is_national_holiday,
# MAGIC     src.is_national_holiday_transferred
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_dim_date
# MAGIC where is_national_holiday is TRUE

# COMMAND ----------


