# Databricks notebook source
# MAGIC %md
# MAGIC ### Psuedo Data Generation 
# MAGIC Utility Notebook to generate psuedo-data for pipeline testing purposes

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optional Analzyer

# COMMAND ----------

import dbldatagen as dg

# Generate synthetic data for store-sales:
sales_source = "/mnt/data/2023-kaggle-final/store-sales/train.csv"
df_data_sample = spark.read.options(header=True, inferSchema=True).csv(sales_source)
analyzer = dg.DataAnalyzer(sparkSession=spark,df=df_data_sample)

display(analyzer.summarizeToDF())


# COMMAND ----------

# MAGIC %md
# MAGIC #### Generate New Data

# COMMAND ----------

# generatedCode = analyzer.scriptDataGeneratorFromData()

import dbldatagen as dg
import pyspark.sql.types

# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=20000,
                     random=True,
                     )
    .withIdOutput()
    .withColumn('date', 'date', expr='date_add(current_date(), cast(floor(rand()*10) as int ))')
    .withColumn('store_nbr', 'int', minValue=1, maxValue=54)
    .withColumn('family', 'string', values=['HOME CARE', 'EGGS', 'DAIRY', 'MEATS', 'BEAUTY', 'BABY CARE'])
    .withColumn('sales', 'double', minValue=0.0, maxValue=500, step=0.5)
    .withColumn('onpromotion', 'int', values=[0,1], weights=[95,5])
    )



# COMMAND ----------

generated_df = generation_spec.build()
display(generated_df)

# COMMAND ----------

import time
import os
import tempfile

# Timestamp for data load:
utc_stamp = round(time.time() * 1000)

# dbfs output path:
output_path = f"/mnt/g5/landingzone/favorita/{utc_stamp}/store_sales.csv"

# Create temporary local file:
with tempfile.NamedTemporaryFile(mode="w+") as f:
  generated_df.toPandas().to_csv(f, header=True)
  print (f"Saving generated data {f.name} to {output_path}")
  dbutils.fs.cp( f"file:{f.name}", output_path )


# COMMAND ----------

# If need to remove generated files
# dbutils.fs.rm("mnt/g5/landingzone/favorita/1701654213708/", True)

# COMMAND ----------


