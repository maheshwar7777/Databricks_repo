# Databricks notebook source
import dlt
import yaml
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from includes.dq_runner import *
from includes.config import *

# COMMAND ----------

def silver_table_factory(table_name):
    @apply_yaml_expectations(table_name, CONFIG_PATH)
    @dlt.table(name=f"{SILVER_DATABASE}.{table_name}")
    def _table():
        df = dlt.read_stream(f"{BRONZE_DATABASE}.{table_name}")
        valid_df, invalid_df = apply_dq(df, table_name, CONFIG_PATH)
        return valid_df
    return _table

@dlt.table(name="common_quarantine", comment="Unified quarantine for all invalid records")
def invalid_quarantine_table():
    invalid_dfs_all_tables = []

    for table_name in TABLE_LIST:
        df = dlt.read_stream(f"{BRONZE_DATABASE}.{table_name}")
        _, invalid_dfs = apply_dq(df, table_name, CONFIG_PATH)
        invalid_dfs_all_tables.extend(invalid_dfs)

    return collect_invalids(invalid_dfs_all_tables)
    
for table in TABLE_LIST:
    silver_table_factory(table)
    #invalid_table_factory(table)