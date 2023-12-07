# Databricks notebook source
# MAGIC %md
# MAGIC ### Train ML Model
# MAGIC This notebook provides support for training, validation and registering model for store-sales predictions.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Synthetic Data for Training Purposes
# MAGIC Using generated data-set for scaffolding

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# generatedCode = analyzer.scriptDataGeneratorFromData()

import dbldatagen as dg
import pyspark.sql.types

# Match expected modeling schema (from ML Exploratory Notebook)

generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=1000000,
                     random=True,
                     )
    .withIdOutput()
    .withColumn('date', 'date', expr='date_add(current_date(), cast(floor(rand()*10) as int ))')
    .withColumn('store_nbr', 'int', minValue=1, maxValue=54)
    .withColumn('family', 'string', values=['HOME CARE', 'EGGS', 'DAIRY', 'MEATS', 'BEAUTY', 'BABY CARE'])
    .withColumn('sales', 'double', minValue=0.0, maxValue=500, step=0.5)
    .withColumn('onpromotion', 'int', values=[0,1], weights=[95,5])
    .withColumn("oil_price", 'double', minValue=30, maxValue=75 )
    .withColumn("is_holiday", 'int', values=[0,1], weights=[95,5])
    )

data_df = generation_spec.build()    

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
  # Model Library (techniques):
  return {
            "rfr": make_pipeline(
                      SimpleImputer(), 
                      RandomForestRegressor(random_state=0, n_estimators=100)),
            "xgbr": XGBRegressor(random_state=0, n_estimators=100)
        }

# COMMAND ----------

def setupForecaster(models):
  # Model Setup:
  return MLForecast(
        models=models,
        freq='D',
        lags=[1,2,4],
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



# Data-set Return Schema:
result_schema =StructType([
  StructField('store_nbr',IntegerType()),
  StructField('family', StringType()),
  StructField('status', BooleanType()),
  StructField('message', StringType())
  ])

@pandas_udf(result_schema, PandasUDFType.GROUPED_MAP)
def build_model(p_df):
  # https://forecastegy.com/posts/multivariate-time-series-forecasting-in-python/
  # Return df values:
  return_df = pd.DataFrame(columns=('store_nbr', 'family', 'status', 'message'))
  store_num = p_df['store_nbr'].iloc[0]
  family = p_df['family'].iloc[0]
  tstamp = round(time.time() * 1000)

  # Adjust Pandas dataframe to default specs:
  p_df['date'] = pd.to_datetime(p_df.date)
  p_df = p_df.drop(columns=['family'])
  p_df = p_df.rename(columns={"date": "ds", "store_nbr":"unique_id", "sales":"y"})

  dynamic_features = ['onpromotion', 'oil_price', 'is_holiday']

  with mlflow.start_run(run_name=f"{store_num}_{family}_{tstamp}"):
    try:

      models = getModels()
      ts_model = setupForecaster(models)
      evaluations = validate_forecaster(models, ts_model, [mape, rmse], p_df)
      rfr_mape = evaluations['rfr']['mape']
      rfr_rmse = evaluations['rfr']['rmse']
      xgbr_mape = evaluations['xgbr']['mape']
      xgbr_rmse = evaluations['xgbr']['rmse']

      # Model Training:
      #ts_model.fit(
      #  p_df, 
      #  id_col='store_nbr', 
      #  time_col='date', 
      #  target_col='sales', 
      #  static_features=[])
      
      # Store mlflow params, etc:
      mlflow.log_param("store", store_num)
      mlflow.log_param("family", family)
      mlflow.log_metric("rfr_mape", rfr_mape )
      mlflow.log_metric("rfr_rmse", rfr_rmse )
      mlflow.log_metric("xgbr_mape", xgbr_mape )
      mlflow.log_metric("xgbr_rmse", xgbr_rmse )
      #mlflow.sklearn.log_model(ts_model, f"{store_num}_{family}_model")

      # Return successful:
      return_df.loc[len(return_df.index)] = [store_num, family, True, ''] 
  
    except Exception as e:
      return_df.loc[len(return_df.index)] = [store_num, family, False, str(e)]
  
  return return_df


# COMMAND ----------

model_output_df = data_df.groupBy( 'store_nbr', 'family').apply(build_model)
display(model_output_df)

# COMMAND ----------


