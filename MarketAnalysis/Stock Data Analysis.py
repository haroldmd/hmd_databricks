# Databricks notebook source
from pyspark.sql.functions import max

# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/tesla_stock_price")

display(df)

# COMMAND ----------

df.select(max("Adj_Close"), max("Volume"))\
  .withColumnRenamed("max(Adj_Close)", "Max Close")\
  .withColumnRenamed("max(Volume)", "Max Volume")\
  .show(truncate=False)

# COMMAND ----------


