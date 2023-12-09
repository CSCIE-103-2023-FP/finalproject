# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS fp_g5.prediction_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE fp_g5.prediction_results (
# MAGIC   ds DATE,
# MAGIC   series_id VARCHAR(100),
# MAGIC   prediction DOUBLE,
# MAGIC   features STRING,
# MAGIC   prediction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS fp_g5.prediction_log;
# MAGIC
# MAGIC CREATE TABLE fp_g5.prediction_log (
# MAGIC   model_name VARCHAR(100),
# MAGIC   status BOOLEAN,
# MAGIC   rmse DOUBLE,
# MAGIC   error_msg STRING,
# MAGIC   prediction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC ) TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC

# COMMAND ----------


