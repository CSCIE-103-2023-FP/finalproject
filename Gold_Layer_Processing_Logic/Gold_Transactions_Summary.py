# Databricks notebook source
# MAGIC %sql
# MAGIC --sum of transactions per month per year
# MAGIC MERGE INTO fp_g5.GOLD_TRANSACTIONS_DTLS USING
# MAGIC (select distinct trans.store_nbr, month(trans.date) as month_transactions_date , year(trans.date) AS year_transactions_date , 
# MAGIC     SUM(TRANSACTIONS) OVER(PARTITION BY trans.store_nbr,month(trans.date) , year(trans.date) 
# MAGIC                                 ORDER BY month(trans.date) desc, year(trans.date)  desc
# MAGIC                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_transactions, _change_type
# MAGIC from fp_g5.silver_dim_transactions trans
# MAGIC INNER JOIN (SELECT distinct store_nbr, month(date) as month_transactions_date, year(date) year_transactions_date, _change_type
# MAGIC             from table_changes('fp_g5.silver_dim_transactions',0)
# MAGIC             where _change_type not in  ('update_preimage', 'delete')) change_trans
# MAGIC on month(trans.date) = change_trans.month_transactions_date
# MAGIC and year(trans.date) = change_trans.year_transactions_date
# MAGIC and change_trans.store_nbr = trans.store_nbr) cdf_silver
# MAGIC on fp_g5.GOLD_TRANSACTIONS_DTLS.month_transactions_date = cdf_silver.month_transactions_date
# MAGIC and fp_g5.GOLD_TRANSACTIONS_DTLS.year_transactions_date = cdf_silver.year_transactions_date
# MAGIC AND fp_g5.GOLD_TRANSACTIONS_DTLS.store_nbr = cdf_silver.store_nbr
# MAGIC when matched and cdf_silver._change_type = 'update_postimage' then
# MAGIC     update set fp_g5.GOLD_TRANSACTIONS_DTLS.total_transactions  = cdf_silver.total_transactions,
# MAGIC                 proc_dt = current_date() 
# MAGIC when not matched then
# MAGIC     insert (store_nbr, month_transactions_date,year_transactions_date, total_transactions, proc_dt) values (store_nbr, month_transactions_date,year_transactions_date, total_transactions, current_date())
