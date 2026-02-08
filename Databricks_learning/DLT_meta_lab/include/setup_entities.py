# Databricks notebook source
# from config import *
# %load_ext autoreload
# %autoreload 2


# COMMAND ----------

# spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")   # dont have access to create catalog

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{DLT_META_DATABASE}")
print(f"Schema created")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.{DLT_META_DATABASE}.{VOLUME_NAME}")
print(f"Volume created")

# COMMAND ----------

def print_config_values():
    """
    Prints the key configuration for the DLT_META
    Helps the user understand where data is stored and how the layers are organized.
    """
    print("Current Pipeline Configuration:\n")
    print(f"Catalog (Unity Catalog)                               : {CATALOG}")
    print(f"Database (Schema) for Incremental Data                : {DLT_META_DATABASE}")
    print(f"Volume for Incremental Data (Storage)                 : {BASE_PATH}")
    print(f"Volume for input file ddl (Storage)                   : {SCHEMA_PATH}")
    print(f"Volume for DQE (Storage)                              : {DQE}")
    print(f"Volume for on boarding json (Storage)                 : {ON_BOARDING_JSON}")
    print(f"Volume for transformation (Storage)                   : {TRANSFORMATION}")
    # print(f"Notebook used in pipeline(store in current directory) : {PIPELINE_NOTEBOOK}")