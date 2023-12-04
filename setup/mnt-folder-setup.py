# Databricks notebook source
# MAGIC %md
# MAGIC ### Create shared data folders
# MAGIC
# MAGIC This notebook creates the required /mnt/ pathed folders to support source data landing in landing zone and delta file creation for bronze layer.

# COMMAND ----------

# MAGIC %fs mkdirs /mnt/g5/

# COMMAND ----------

# MAGIC %fs mkdirs /mnt/g5/landingzone

# COMMAND ----------

# MAGIC %fs mkdirs /mnt/g5/landingzone/favorita

# COMMAND ----------

# MAGIC %fs mkdirs /mnt/g5/landingzone/government

# COMMAND ----------

# MAGIC %fs mkdirs /mnt/g5/bronze

# COMMAND ----------

# MAGIC %fs ls /mnt/g5

# COMMAND ----------

# MAGIC %fs ls /mnt/g5/landingzone

# COMMAND ----------


