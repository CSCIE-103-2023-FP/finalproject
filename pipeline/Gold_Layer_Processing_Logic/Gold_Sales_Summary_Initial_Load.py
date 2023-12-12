# Databricks notebook source
# MAGIC %sql
# MAGIC --sum of total sales  per month per store nbr per product family - sales info
# MAGIC MERGE INTO fp_g5.GOLD_SALES_DTLS USING
# MAGIC (select distinct train.store_nbr AS STORE_NBR, train.family AS FAMILY, month(train.date) as month_train_date, year(train.date) year_train_date, 
# MAGIC     sum(sales) OVER(PARTITION BY train.store_nbr,train.family, month(train.date), year(train.date)
# MAGIC                                 ORDER BY month(train.date) desc, year(train.date) desc
# MAGIC                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_sales, _change_type
# MAGIC from fp_g5.silver_fact_train train
# MAGIC INNER JOIN (SELECT distinct STORE_NBR, family , month(date) as month_change_train_date, year(date) year_change_train_date,
# MAGIC             _change_type
# MAGIC             from table_changes( 'fp_g5.silver_fact_train',0)
# MAGIC             where _change_type not in  ('update_preimage', 'delete')) as change_train
# MAGIC on train.store_nbr = change_train.store_nbr
# MAGIC and train.family = change_train.family
# MAGIC and month(train.date) = change_train.month_change_train_date
# MAGIC and year(train.date) = change_train.year_change_train_date
# MAGIC ) cdf_silver
# MAGIC on fp_g5.GOLD_SALES_DTLS.store_nbr = cdf_silver.store_nbr
# MAGIC and fp_g5.GOLD_SALES_DTLS.family = cdf_silver.family
# MAGIC and fp_g5.GOLD_SALES_DTLS.month_train_date = cdf_silver.month_train_date
# MAGIC and fp_g5.GOLD_SALES_DTLS.year_train_date = cdf_silver.year_train_date
# MAGIC when matched and cdf_silver._change_type = 'update_postimage' then
# MAGIC     update set fp_g5.GOLD_SALES_DTLS.total_sales = cdf_silver.total_sales, fp_g5.GOLD_SALES_DTLS.proc_dt = current_date() 
# MAGIC when not matched then
# MAGIC     insert (store_nbr,family, month_train_date, year_train_date, total_sales, proc_dt) values (store_nbr,family, month_train_date, year_train_date, total_sales, current_date())
