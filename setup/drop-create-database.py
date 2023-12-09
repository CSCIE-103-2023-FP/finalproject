# Databricks notebook source
databaseName = "fp_g5"

print('databaseName ' + databaseName)

spark.sql(f"DROP DATABASE IF EXISTS {databaseName} CASCADE")
spark.sql(f"CREATE DATABASE {databaseName}")
spark.sql(f"use {databaseName}")

print (f"Database {databaseName} successfully rebuilt.")

# COMMAND ----------


