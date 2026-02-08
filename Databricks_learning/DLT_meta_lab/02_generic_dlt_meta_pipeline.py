# Databricks notebook source
# MAGIC %pip install dlt-meta

# COMMAND ----------

# Generic DLT_META commands to create bronze and silver layer tables

from src.dataflow_pipeline import DataflowPipeline

layer = spark.conf.get("layer", None)
DataflowPipeline.invoke_dlt_pipeline(spark, layer)