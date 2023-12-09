# Databricks notebook source
# MAGIC %md
# MAGIC # Batch and Structured Streaming for Silver dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_dim_product_family
# MAGIC * Treat this as a reference table of product families
# MAGIC * daily/ batch approach would be to check if there are any new family of products which are not there
# MAGIC   * if not found insert
# MAGIC   * if found ignore
# MAGIC   * streaming is NOT considered because we do not expect new products families to be launched every day

# COMMAND ----------

# MAGIC %sql
# MAGIC USE fp_g5;

# COMMAND ----------

# MAGIC %sql
# MAGIC /* this table has a generated column for product_family_nbr
# MAGIC so for change data, we only need to insert new products found, if any
# MAGIC from teh incoming bronze table
# MAGIC */
# MAGIC INSERT INTO silver_dim_product_family (family)
# MAGIC   SELECT DISTINCT
# MAGIC     family
# MAGIC   FROM bronze_train src
# MAGIC   WHERE src.family NOT IN (SELECT DISTINCT family FROM
# MAGIC   silver_dim_product_family)

# COMMAND ----------


