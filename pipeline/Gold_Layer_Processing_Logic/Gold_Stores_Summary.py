# Databricks notebook source
# MAGIC %sql
# MAGIC --Store Details Final
# MAGIC MERGE INTO fp_g5.GOLD_STORES_DTLS USING
# MAGIC (SELECT distinct store_nbr, city, state, type, cluster,  _change_type 
# MAGIC             from table_changes('fp_g5.silver_dim_store',0)
# MAGIC             WHERE date(_COMMIT_TIMESTAMP) = CURRENT_DATE() ) cdf_silver
# MAGIC on  (fp_g5.GOLD_STORES_DTLS.store_nbr = cdf_silver.store_nbr
# MAGIC and fp_g5.GOLD_STORES_DTLS.stat = cdf_silver._change_type)
# MAGIC OR (fp_g5.GOLD_STORES_DTLS.store_nbr = cdf_silver.store_nbr
# MAGIC and cdf_silver._change_type =  'delete')
# MAGIC when matched and cdf_silver._change_type in ('update_preimage', 'delete') then
# MAGIC     update set  active_ind = 'N', expiry_timestmp = current_date(), stat = 'inactive', proc_dt = current_date()
# MAGIC when not matched then
# MAGIC     insert (store_nbr, city,state, type, cluster, active_ind, expiry_timestmp, stat, proc_dt)  values (cdf_silver.store_nbr, cdf_silver.city,cdf_silver.state, cdf_silver.type, cdf_silver.cluster, 'Y', '9999-12-31', 'update_preimage', current_date());
