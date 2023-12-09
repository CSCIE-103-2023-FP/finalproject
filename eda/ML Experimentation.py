# Databricks notebook source
# MAGIC %md 
# MAGIC ### ML Experimentation
# MAGIC Notebook for general algorithm experimentation
# MAGIC
# MAGIC _Note:_ Prophet/Orbit installation library management was determined to have issues, so multi-variate time series approach from scikit used instead.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Kaggle Data-set

# COMMAND ----------

from pyspark.sql import functions as F

store_sales_df = (spark
              .read
              .option("header", True).option("inferSchema",True)
              .csv("dbfs:/mnt/data/2023-kaggle-final/store-sales/train.csv"))

store_master_df = (spark
              .read
              .option("header", True).option("inferSchema",True)
              .csv("dbfs:/mnt/data/2023-kaggle-final/store-sales/stores.csv"))

holiday_events_df = (spark
                      .read
                      .option("header", True).option("inferSchema",True)
                      .csv("dbfs:/mnt/data/2023-kaggle-final/store-sales/holidays_events.csv"))
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

oil_prices_df = (spark
                  .read
                  .option("header", True).option("inferSchema",True)
                  .csv("dbfs:/mnt/data/2023-kaggle-final/store-sales/oil.csv")).withColumnRenamed('dcoilwtico', 'oil_price')

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

display(encoded_df)

# COMMAND ----------

# encoded_df.write.mode("overwrite").saveAsTable("fp_g5.modeling_input")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert to Pandas

# COMMAND ----------

import pandas as pd

p_df = encoded_df.toPandas()
p_df['oil_price'] = p_df['oil_price'].fillna(method='bfill')
p_df['date'] = pd.to_datetime(p_df.date)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Select specific store/sku combination

# COMMAND ----------

import matplotlib.pyplot as plt

# Select Store & Family for specific trend:
store_nbr = 18
family = "HOME CARE"

p_data = p_df[ 
              (p_df["store_nbr"] == store_nbr) &
              (p_df["family"] == family ) & 
              (p_df["date"] > '2017-01-01' ) ]

# p_data.groupby("family").plot('date','sales')
p_data.plot('date', 'sales')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build time-series model

# COMMAND ----------

# MAGIC %pip install mlforecast

# COMMAND ----------

# Static features:
store_characteristics = ['type_A', 'type_B', 'type_C', 'type_D', 
                   'cluster_1', 'cluster_2','cluster_3','cluster_4', 'cluster_6', 'cluster_7', 'cluster_8', 'cluster_9',
                   'cluster_10','cluster_11','cluster_12', 'cluster_13','cluster_14','cluster_15','cluster_16','cluster_17'] # store static features
p_data = p_data.drop(columns=store_characteristics, errors='ignore')

# Column renaming to MLForecast defaults:
p_data['unique_id'] = p_data['family'] + "_" + p_data['store_nbr'].astype(str)
p_data = p_data.rename(columns={"date": "ds", "sales":"y"})

# Static or transformed features:
p_data = p_data.drop(columns=['family', 'store_nbr', 'city','state','id', 'national_holiday','regional_holiday','local_holiday'], errors='ignore')

p_data

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
from xgboost import XGBRegressor
from mlforecast import MLForecast
from window_ops.rolling import rolling_mean, rolling_max, rolling_min

# https://forecastegy.com/posts/multivariate-time-series-forecasting-in-python/
# https://nixtlaverse.nixtla.io/mlforecast/docs/tutorials/electricity_load_forecasting.html

static_features = []
dynamic_features = ['onpromotion', 'oil_price', 'is_holiday']

models = {
            "rfr": make_pipeline(
              SimpleImputer(), 
              RandomForestRegressor(random_state=0, n_estimators=100)),
            "xgbr": XGBRegressor(random_state=0, n_estimators=100)
        }

ts_model = MLForecast(
            models=models,
            freq='D',
            lags=[8,9,10,11,12],
            lag_transforms={
              1: [(rolling_mean, 4), (rolling_min, 4), (rolling_max, 4)],
            },
            date_features=['week', 'month'],
            num_threads=6)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Evaluation

# COMMAND ----------

c_df = ts_model.cross_validation(
  df = p_data,
  h=7,
  n_windows=4,
  refit=False
)

c_df.head()

# COMMAND ----------

def plot_cv(df, df_cv, uid, last_n=24 * 14):
    cutoffs = df_cv.query('unique_id == @uid')['cutoff'].unique()
    fig, ax = plt.subplots(nrows=len(cutoffs), ncols=1, figsize=(14, 6), gridspec_kw=dict(hspace=0.8))
    for cutoff, axi in zip(cutoffs, ax.flat):
        df.query('unique_id == @uid').tail(last_n).set_index('ds').plot(ax=axi, title=uid, y='y')
        df_cv.query('unique_id == @uid & cutoff == @cutoff').set_index('ds').plot(ax=axi, title=uid, y='rfr')
    
plot_cv(p_data, c_df, 'HOME CARE_18')


# COMMAND ----------

from utilsforecast.losses import *
from utilsforecast.evaluation import evaluate

metrics = [mape, rmse]

# Function to evaluate the crossvalidation
def evaluate_crossvalidation(crossvalidation_df, metrics, models):
    evaluations = []
    for c in crossvalidation_df['cutoff'].unique():
        df_cv = crossvalidation_df.query('cutoff == @c')
        evaluation = evaluate(
            df = df_cv,
            metrics=metrics,
            models=list(models.keys())
            )
        evaluations.append(evaluation)
    evaluations = pd.concat(evaluations, ignore_index=True).drop(columns='unique_id')
    evaluations = evaluations.groupby('metric').mean()
    return evaluations
  
evaluations = evaluate_crossvalidation(c_df, metrics, models)
evaluations

# COMMAND ----------

evaluations['rfr']['mape']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Predictions

# COMMAND ----------

ts_model.fit(
  p_data,
  static_features=static_features,
  max_horizon=14)

# COMMAND ----------

p_data.tail()

# COMMAND ----------

prep = ts_model.preprocess(p_data)
prep

# COMMAND ----------

import shap

prep = ts_model.preprocess(p_data)
X = prep.drop(columns=['unique_id','ds', 'y'])

X100 = shap.utils.sample(X, 100)
explainer = shap.Explainer(ts_model.models_['rfr'][0].predict, X100)
shap_values = explainer(X)

shap.plots.beeswarm(shap_values)

# COMMAND ----------

import pandas as pd

x_df_data = [
  ["HOME CARE_18", max_ts + datetime.timedelta(days=1), 21, 47.57,1],
  ["HOME CARE_18", max_ts + datetime.timedelta(days=2), 21, 47.57,1],
]

x_df = pd.DataFrame(x_df_data, columns=['unique_id', 'ds','onpromotion','oil_price','is_holiday'])
x_df

# COMMAND ----------

future_ds = ts_model.make_future_dataframe(7)
future_ds['onpromotion'] = 0
future_ds['oil_price'] = 50
future_ds['is_holiday'] = 0
future_ds

# COMMAND ----------

ts_model.predict( 7, X_df=future_ds)

# COMMAND ----------

ts_model

# COMMAND ----------


