# Databricks notebook source
# MAGIC %sql
# MAGIC --Holiday Events Details
# MAGIC MERGE INTO fp_g5.Gold_Holiday_Events_Dtls USING
# MAGIC (SELECT distinct date, type, locale, locale_name, description,transferred, _change_type
# MAGIC             from table_changes('fp_g5.silver_dim_holidays_events',2)
# MAGIC             WHERE date(_COMMIT_TIMESTAMP) = CURRENT_DATE() ) cdf_silver
# MAGIC on  (fp_g5.Gold_Holiday_Events_Dtls.date = cdf_silver.date
# MAGIC and fp_g5.Gold_Holiday_Events_Dtls.type = cdf_silver.type
# MAGIC and fp_g5.Gold_Holiday_Events_Dtls.locale = cdf_silver.locale
# MAGIC and fp_g5.Gold_Holiday_Events_Dtls.locale_name = cdf_silver.locale_name
# MAGIC and fp_g5.Gold_Holiday_Events_Dtls.stat = cdf_silver._change_type)
# MAGIC OR (fp_g5.Gold_Holiday_Events_Dtls.date = cdf_silver.date
# MAGIC and fp_g5.Gold_Holiday_Events_Dtls.type = cdf_silver.type
# MAGIC and fp_g5.Gold_Holiday_Events_Dtls.locale = cdf_silver.locale
# MAGIC and fp_g5.Gold_Holiday_Events_Dtls.locale_name = cdf_silver.locale_name
# MAGIC and cdf_silver._change_type = 'delete')
# MAGIC when matched and cdf_silver._change_type in ('update_preimage', 'delete') then
# MAGIC     update set  active_ind = 'N', expiry_timestmp = current_date(), stat = 'inactive', proc_dt = current_date()
# MAGIC when not matched then
# MAGIC     insert (date, type,locale, locale_name, description,transferred, active_ind, expiry_timestmp, stat, proc_dt)  values (date, type,locale, locale_name, description,transferred, 'Y', '9999-12-31', 'update_preimage', current_date());
