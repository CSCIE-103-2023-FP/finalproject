# Databricks notebook source
# MAGIC %md
# MAGIC ### Train ML Model
# MAGIC This notebook provides support for training, validation and registering model for store-sales predictions.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Training Data from Silver Layer
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Most recent 6 months of data:
data_source = "fp_g5.modeling_input"
data_df = spark \
           .sql(f"Select date, store_nbr, family, sales, onpromotion, oil_price,is_holiday from {data_source}") \
            .withColumn("max_date", F.max("date").over(Window.orderBy(F.lit(1)))) \
            .filter(F.col("date") >= F.add_months(F.col("max_date"), -12)) \
            .filter(F.col("date") >= "2017-01-01") \
            .filter(F.col("store_nbr") >= 4) \
            .drop("max_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parallelized Model Training

# COMMAND ----------

# MAGIC %pip install mlforecast

# COMMAND ----------

# Imports
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

import mlflow
import time

from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
from xgboost import XGBRegressor
from mlforecast import MLForecast
from window_ops.rolling import rolling_mean, rolling_max, rolling_min

# COMMAND ----------

def getModels():
  # Best performing overall model from Exploratory Modeling:
  return {
            "rfr": make_pipeline(
                      SimpleImputer(),
                      RandomForestRegressor(random_state=0, n_estimators=100))
        }

# COMMAND ----------

def setupForecaster(models):
  # Model Setup:
  return MLForecast(
        models=models,
        freq='D',
        lags=[8,9,10,11,12],
        lag_transforms={
          1: [(rolling_mean, 4), (rolling_min, 4), (rolling_max, 4)],
        },
        date_features=['week', 'month'],
        num_threads=6)


# COMMAND ----------

from utilsforecast.losses import *
from utilsforecast.evaluation import evaluate

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
  
def validate_forecaster( models, ts_model, metrics, p_df ):
    # Validation (n-folds):
    c_df = ts_model.cross_validation(
      df = p_df,
      h=7,
      n_windows=1,
      refit=False
    )

    evaluations = evaluate_crossvalidation(c_df, metrics, models)
    return evaluations


# COMMAND ----------

from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository

def register_and_update_model( model_name, runinfo ):
  client = mlflow.tracking.MlflowClient()
  registered_model = client.create_registered_model(model_name)
  # Update model version
  runs_uri = "runs:/" + runinfo.run_uuid + f"/{model_name}"
  model_src = RunsArtifactRepository.get_underlying_uri(runs_uri)
  client.create_model_version(model_name, model_src, runinfo.run_id)

# COMMAND ----------

# Data-set Return Schema:
result_schema =StructType([
  StructField('model_name',StringType()),
  StructField('status', BooleanType()),
  StructField('rmse', DoubleType()),
  StructField('message', StringType())
  ])

@pandas_udf(result_schema, PandasUDFType.GROUPED_MAP)
def build_model(p_df):
  # https://forecastegy.com/posts/multivariate-time-series-forecasting-in-python/
  
  # Return df values:
  return_df = pd.DataFrame(columns=('model_name', 'status', 'rmse', 'message'))
  store_num = p_df['store_nbr'].iloc[0]
  family = p_df['family'].iloc[0]
  tstamp = round(time.time() * 1000)

  # Adjust Pandas dataframe to default specs:
  p_df['date'] = pd.to_datetime(p_df.date)
  p_df['unique_id'] = p_df['store_nbr'].astype(str) + "_" + p_df['family']
  p_df = p_df.rename(columns={"date": "ds", "sales":"y"})
  p_df = p_df.drop(columns=['family', 'store_nbr'], errors='ignore')

  model_name = f"{store_num}_{family}_model"

  static_features = []
  dynamic_features = ['onpromotion', 'oil_price', 'is_holiday']

  with mlflow.start_run(run_name=f"{store_num}_{family}_{tstamp}") as run:
    try:

      models = getModels()
      ts_model = setupForecaster(models)
      evaluations = validate_forecaster(models, ts_model, [rmse], p_df)
      rfr_rmse = evaluations['rfr']['rmse']
      
      # Model Training:
      ts_model.fit(
        p_df,
        static_features=static_features,
        max_horizon=21)
      
      # Store mlflow params, etc:
      mlflow.log_param("store", store_num)
      mlflow.log_param("family", family)
      mlflow.log_metric("rfr_rmse", rfr_rmse )
      
      # Potentially: if a > b: ts_model.models_["a"][0]
      mlflow.sklearn.log_model(ts_model, model_name, registered_model_name=model_name )

      # Store updated best performing model in Registry:
      #register_and_update_model( model_name, run.info )

      # Return successful:
      return_df.loc[len(return_df.index)] = [model_name, True, rfr_rmse, ''] 
  
    except Exception as e:
      return_df.loc[len(return_df.index)] = [model_name, False, 0, str(e)]
  
  return return_df


# COMMAND ----------

from pyspark.sql import functions as F
# .filter( (F.col('store_nbr') == 18) & (F.col('family') == 'HOME CARE') ) \                    
model_output_df = data_df \
                    .filter( (F.col('store_nbr') <= 18) & (F.col('family') == 'HOME CARE') ) \
                    .groupBy('store_nbr', 'family') \
                    .apply(build_model)
display(model_output_df)

# COMMAND ----------

tbl_name = "fp_g5.prediction_log"
df = model_output_df \
      .withColumnRenamed("message", "error_msg") \

df.write.format("delta").mode("append").saveAsTable(tbl_name)

# COMMAND ----------


