# Databricks notebook source
# MAGIC %sql
# MAGIC --Holiday Events Details
# MAGIC MERGE INTO fp_g5.Gold_Holiday_Events_Dtls USING
# MAGIC (SELECT distinct date, type, locale, locale_name, description,transferred, _change_type
# MAGIC             from table_changes('fp_g5.silver_dim_holidays_events',0) ) cdf_silver
# MAGIC on  (fp_g5.Gold_Holiday_Events_Dtls.date = cdf_silver.date
# MAGIC and fp_g5.Gold_Holiday_Events_Dtls.type = cdf_silver.type
# MAGIC and fp_g5.Gold_Holiday_Events_Dtls.locale = cdf_silver.locale
# MAGIC and fp_g5.Gold_Holiday_Events_Dtls.locale_name = cdf_silver.locale_name
# MAGIC and cdf_silver._change_type in ('update_preimage', 'delete'))
# MAGIC when matched and cdf_silver._change_type in ('update_preimage', 'delete') then
# MAGIC     update set  active_ind = 'N', expiry_timestmp = current_date(), proc_dt = current_date()
# MAGIC when not matched then
# MAGIC     insert (date, type,locale, locale_name, description,transferred, active_ind, expiry_timestmp,  proc_dt)  values (date, type,locale, locale_name, description,transferred, 'Y', '9999-12-31',  current_date());
# MAGIC
# MAGIC
# MAGIC
