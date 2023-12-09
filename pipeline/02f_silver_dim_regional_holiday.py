# Databricks notebook source
# MAGIC %md
# MAGIC # Batch and Structured Streaming for Silver dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_regional_holiday
# MAGIC * the composite key `date`, `type` and `locale` form a unique combination
# MAGIC * the upsert is made based on that
# MAGIC * treating this as a SCD1, batch mode

# COMMAND ----------

# MAGIC %sql
# MAGIC USE fp_g5;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_dim_regional_holiday tgt
# MAGIC USING (
# MAGIC   SELECT DISTINCT
# MAGIC     h.*
# MAGIC   FROM (SELECT DISTINCT * FROM bronze_holidays_events) AS h
# MAGIC   WHERE
# MAGIC     h.locale != 'National'
# MAGIC     AND NOT h.transferred
# MAGIC   ORDER BY
# MAGIC     h.date) src
# MAGIC ON tgt.date = src.date
# MAGIC and tgt.type = src.type
# MAGIC and tgt.locale = src.locale
# MAGIC and tgt.locale_name = src.locale_name
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.description = src.description,
# MAGIC     tgt.transferred = src.transferred
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     `date`,
# MAGIC     `type`,
# MAGIC     locale,
# MAGIC     locale_name,
# MAGIC     description,
# MAGIC     transferred
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.`date`,
# MAGIC     src.`type`,
# MAGIC     src.locale,
# MAGIC     src.locale_name,
# MAGIC     src.description,
# MAGIC     src.transferred
# MAGIC   )

# COMMAND ----------


