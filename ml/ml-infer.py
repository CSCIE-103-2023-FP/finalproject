# Databricks notebook source
# MAGIC %pip install mlforecast

# COMMAND ----------

tbl_name = "fp_g5.prediction_results"

store_nbr = 18
family = "BABY CARE"
model_name = f"{store_nbr}_{family}_model"


# COMMAND ----------

from mlflow.tracking.client import MlflowClient
client = MlflowClient()
model_version = client.get_latest_versions(model_name)[0].version
model_uri = "models:/{model_name}/{model_version}".format(model_name=model_name, model_version=model_version)
model_uri

# COMMAND ----------

import mlflow
import mlforecast

# Retrieve model
latest_prod_model = mlflow.sklearn.load_model(model_uri)


# COMMAND ----------

import json

X_df = latest_prod_model.make_future_dataframe(7)
onpromo = 0
is_holiday = 0
oil_price = 50

X_df["onpromotion"] = onpromo
X_df["is_holiday"] = is_holiday
X_df["oil_price"] = oil_price

predictions = latest_prod_model.predict( 7, X_df = X_df )

features = {'onpromotion': onpromo, 'is_holiday': is_holiday, 'oil_price': oil_price}
predictions['features'] = json.dumps(features)

predictions

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame(predictions)
df = df \
      .withColumnRenamed("rfr", "prediction") \
      .withColumnRenamed("unique_id", "series_id")\
      .withColumn("ds", F.to_date("ds"))\
      .drop("rfr")
display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable(tbl_name)

# COMMAND ----------


