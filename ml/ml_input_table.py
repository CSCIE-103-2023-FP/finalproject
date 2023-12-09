# Databricks notebook source
# MAGIC %md
# MAGIC #### Model Input Layer
# MAGIC Builds silver layer model_input table for use in modeling pipelines

# COMMAND ----------

from pyspark.sql import functions as F

store_sales_df = spark.sql("Select * from fp_g5.bronze_train")
store_master_df = spark.sql("Select * from fp_g5.bronze_stores")
holiday_events_df = spark.sql("Select * from fp_g5.bronze_holidays_events")
oil_prices_df = spark.sql("Select * from fp_g5.bronze_oil_prices").withColumnRenamed('dcoilwtico', 'oil_price')


# COMMAND ----------

holiday_events_df = holiday_events_df.withColumn('city', holiday_events_df['locale_name'])
holiday_events_df = holiday_events_df.withColumn('state', holiday_events_df['locale_name'])

local_holiday_df = (holiday_events_df
                        .filter(holiday_events_df['locale'] == 'Local')
                        .select(['date','city','transferred'])
                        .withColumn(
                          'local_holiday', 
                          F.when( F.col("transferred") == F.lit(True), 0)
                          .otherwise(1) )
                      ).drop('transferred')

regional_holiday_df = (holiday_events_df
                        .filter(holiday_events_df['locale'] == 'Regional')
                        .select(['date','state','transferred'])
                        .withColumn(
                          'regional_holiday', 
                          F.when( F.col("transferred") == F.lit(True), 0)
                          .otherwise(1) )
                      ).drop('transferred')

national_holiday_df = (holiday_events_df
                        .filter(holiday_events_df['locale'] == 'National')
                        .select(['date','transferred'])
                        .withColumn(
                          'national_holiday', 
                          F.when( F.col("transferred") == F.lit(True), 0)
                          .otherwise(1) )
                      ).drop('transferred')

# COMMAND ----------

# Joined data-frames:
joined_df = store_sales_df.join(store_master_df, ['store_nbr'], 'left') # Add store-master data
joined_df = joined_df.join(oil_prices_df, ['date'], 'left') # Add oil price data

# Joined holidays:
joined_df = joined_df.join(national_holiday_df, ['date'], 'left') # Add national holidays
joined_df = joined_df.join(regional_holiday_df, ['date', 'state'], 'left') # Add regional holidays     
joined_df = joined_df.join(local_holiday_df, ['date', 'city'], 'left') # Add local holidays     
joined_df = joined_df.fillna(0, subset=['national_holiday', 'regional_holiday', 'local_holiday'])
joined_df = joined_df.withColumn('is_holiday', 
                        F.greatest( 
                              F.col("national_holiday"), 
                              F.col("regional_holiday"), 
                              F.col("local_holiday") 
                        ))

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import StringIndexer
from pyspark.ml.functions import vector_to_array


# String indexing
indexer = StringIndexer(inputCols=['type','cluster'], outputCols=['type_numeric', 'cluster_numeric'])
indexer_fitted = indexer.fit(joined_df)
indexed_df = indexer_fitted.transform(joined_df)

# One-hot encoding:
encoder = OneHotEncoder(inputCols=["type_numeric", "cluster_numeric"],
                        outputCols=["type_vec", "cluster_vec"])
encoder_model = encoder.fit(indexed_df)
encoded_df = encoder_model.transform(indexed_df)
encoded_df = encoded_df.drop('type','cluster','type_numeric','cluster_numeric')

# Dummy columns:
encoded_df = encoded_df.select(
                  '*', 
                  vector_to_array('cluster_vec').alias('cluster_array'), 
                  vector_to_array('type_vec').alias('type_array')).drop('cluster_vec', 'type_vec')

def expand_array( df, col_name, lbls, prefix='' ):
  num_categories = len(df.first()[col_name])
  cols_expanded = [(F.col(col_name)[i].alias(f"{prefix}{lbls[i]}")) for i in range(num_categories)]
  return df.select('*', *cols_expanded)

encoded_df = expand_array (encoded_df, 'type_array', indexer_fitted.labelsArray[0], 'type_').drop('type_array')
encoded_df = expand_array (encoded_df, 'cluster_array', indexer_fitted.labelsArray[1], 'cluster_').drop('cluster_array')

# COMMAND ----------

display( encoded_df )

# COMMAND ----------

tbl_name = "fp_g5.modeling_input"

# Overwrite modeling_input with current joins:
encoded_df.write.mode("overwrite").saveAsTable(tbl_name)

# COMMAND ----------


