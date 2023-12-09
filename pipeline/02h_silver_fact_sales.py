# Databricks notebook source
# MAGIC %md
# MAGIC # Batch and Structured Streaming for Silver dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_fact_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC USE fp_g5;
# MAGIC
# MAGIC MERGE INTO fp_g5.silver_fact_sales t
# MAGIC USING (SELECT
# MAGIC   `date`,
# MAGIC   store_nbr,
# MAGIC   pf.product_family_nbr,
# MAGIC   sales,
# MAGIC   onpromotion
# MAGIC FROM bronze_train AS bt
# MAGIC INNER JOIN silver_dim_product_family AS pf ON
# MAGIC   bt.family = pf.family) s
# MAGIC ON t.date = s.date
# MAGIC AND t.store_nbr = s.store_nbr
# MAGIC AND t.product_family_nbr = s.product_family_nbr
# MAGIC WHEN MATCHED THEN UPDATE SET 
# MAGIC     t.sales = s.sales,
# MAGIC     t.onpromotion = s.onpromotion
# MAGIC     WHEN NOT MATCHED THEN INSERT 
# MAGIC     (`date`,
# MAGIC     store_nbr, product_family_nbr,
# MAGIC     sales, onpromotion)
# MAGIC     VALUES 
# MAGIC     (
# MAGIC     s.date,
# MAGIC     s.store_nbr,
# MAGIC     s.product_family_nbr,
# MAGIC     s.sales,
# MAGIC     s.onpromotion) 
