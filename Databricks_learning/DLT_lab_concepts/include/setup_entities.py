# Databricks notebook source
# from config import *
# %load_ext autoreload
# %autoreload 2


# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {C_CATALOG}")   # dont have access to create catalog

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C_CATALOG}.{DLT_DATABASE}")
print(f"Schema created")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {C_CATALOG}.{DLT_DATABASE}.{VOLUME_NAME}")
print(f"Volume created")

# COMMAND ----------

def print_config_values():
    """
    Prints the key configuration for the DLT_META
    Helps the user understand where data is stored and how the layers are organized.
    """
    print("Current Pipeline Configuration:\n")
    print(f"Catalog (Unity Catalog)                               : {C_CATALOG}")
    print(f"Database (Schema) for Incremental Data                : {DLT_DATABASE}")
    print(f"Volume for Incremental Data (Storage)                 : {VOLUME_NAME}")

    # print(f"Notebook used in pipeline(store in current directory) : {PIPELINE_NOTEBOOK}")