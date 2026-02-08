# Databricks notebook source
from include.config import *
%load_ext autoreload
%autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Pass the entities as parameter

# COMMAND ----------


src_entity=[]

for ent in TABLE_LIST:
    src_entity.append({"src" : ent})

print(src_entity)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="output_key", value=src_entity)