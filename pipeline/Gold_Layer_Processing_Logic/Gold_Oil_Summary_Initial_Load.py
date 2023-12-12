# Databricks notebook source
# MAGIC %sql
# MAGIC --average oil proce per month per year
# MAGIC MERGE INTO fp_g5.GOLD_OIL_PRICE_DTLS USING
# MAGIC (select distinct month(oil.date) as MONTH_OIL_PRICE , year(oil.date) YEAR_OIL_PRICE , 
# MAGIC     avg(oil_price) OVER(PARTITION BY month(oil.date) , year(oil.date) 
# MAGIC                                 ORDER BY month(oil.date) desc, year(oil.date)  desc
# MAGIC                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS oil_price, _change_type
# MAGIC from fp_g5.silver_dim_oil_prices oil
# MAGIC INNER JOIN (SELECT distinct month(date) as month_oil_date, year(date) year_oil_date, _change_type
# MAGIC             from table_changes('fp_g5.silver_dim_oil_prices',0)
# MAGIC             where _change_type not in  ('update_preimage', 'delete')) change_oil
# MAGIC on month(oil.date) = change_oil.month_oil_date
# MAGIC and year(oil.date) = change_oil.year_oil_date) cdf_silver
# MAGIC on fp_g5.GOLD_OIL_PRICE_DTLS.MONTH_OIL_PRICE = cdf_silver.MONTH_OIL_PRICE
# MAGIC and fp_g5.GOLD_OIL_PRICE_DTLS.YEAR_OIL_PRICE = cdf_silver.YEAR_OIL_PRICE
# MAGIC when matched and cdf_silver._change_type = 'update_postimage' then
# MAGIC     update set fp_g5.GOLD_OIL_PRICE_DTLS.oil_price  = cdf_silver.oil_price , 
# MAGIC                 fp_g5.GOLD_OIL_PRICE_DTLS.proc_dt = current_date()
# MAGIC when not matched then
# MAGIC     insert (MONTH_OIL_PRICE,YEAR_OIL_PRICE, oil_price, proc_dt) values (MONTH_OIL_PRICE,YEAR_OIL_PRICE, oil_price, current_date())
