# Databricks notebook source
from config import *

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{RAW_DATABASE}")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.{RAW_DATABASE}.{VOLUME_NAME}")