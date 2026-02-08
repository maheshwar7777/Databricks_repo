# Databricks notebook source
# MAGIC %md
# MAGIC ## DQX Integration in Streaming Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoloader
# MAGIC Databricks Autoloader is an optimized cloud file source for Apache Spark Structured Streaming. It's designed for the automated and incremental ingestion of new data files as they arrive in cloud object storage (like S3, ADLS, or GCS) into Delta tables.
# MAGIC -   **Features**:
# MAGIC       -     Monitors storage Location
# MAGIC       -     Tracks progress
# MAGIC       -     Loads data
# MAGIC       -     Resumes automatically

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing the DQX Library
# MAGIC
# MAGIC So, Installs the DQX library, which provides tools for profiling data, generating data quality rules and applying those rules to Spark DataFrames.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restart Python Kernel
# MAGIC
# MAGIC Restarts the Python kernel to ensure the newly installed package is available for import.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing required libraries
# MAGIC - Imports Spark and DQX modules for data manipulation, profiling, rule generation, and rule enforcement.
# MAGIC - yaml is used for handling YAML-formatted rule definitions.
# MAGIC - logging is for logging information.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from delta.tables import DeltaTable
from pyspark.sql.functions import lit

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx import check_funcs 

from databricks.sdk import WorkspaceClient

from datetime import date, datetime, time
import yaml
import logging

# COMMAND ----------

# MAGIC %md
# MAGIC Setting up logging

# COMMAND ----------

logging.basicConfig(level=logging.INFO)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initializing catalog & schema 

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG uc_catalog;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Note: Run cell 12 & 13 only If you have fresh run setup !!!
# MAGIC ### Dropping Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table bronze_dqx;
# MAGIC -- drop table silver_dqx;
# MAGIC -- drop table quarantine_dqx;
# MAGIC -- drop view gold_dqx;

# COMMAND ----------

# dbutils.fs.rm("/Volumes/uc_catalog/default/dqx_managed/checkpoint",True)
# dbutils.fs.rm("/Volumes/uc_catalog/default/dqx_managed/schema",True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Intializing parameters

# COMMAND ----------

#Initialize variables
volum_path = "/Volumes/uc_catalog/default/dqx_managed"
bronze_table = "bronze_dqx"
silver_table = "silver_dqx"
quarantine_table = "quarantine_dqx"
gold_table = "gold_dqx"

# COMMAND ----------

# MAGIC %md
# MAGIC Place/Put **.databrickscfg** file in volum or accessible path.
# MAGIC
# MAGIC **Example .databrickscfg file content:**
# MAGIC       
# MAGIC       [DEFAULT]
# MAGIC       host  = workspace_link
# MAGIC       token = dapixxxxxxxxxxxxxxxxxxxxxxxxxx 
# MAGIC
# MAGIC token refers to personal access token that can be fetch from developer option in settings
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Intialize Streamin**g to read raw data from volum path(It can be any cloud object storage (like S3, ADLS, or GCS)).
# MAGIC Raw data files uploaded in **bronze tables & mark in checkpoint** location this ensure that in next incremental run only fresh files will be pick for processing 
# MAGIC

# COMMAND ----------

df_autoload = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{volum_path}/schema") 
    .option("cloudFiles.schemaEvolutionMode", "rescue") 
    .option("cloudFiles.rescuedDataColumn", "_rescued_data") 
    .option("readChangeFeed", "true") 
    .option("mergeSchema","true") \
    .load(f"{volum_path}/input")
)


query = (df_autoload.writeStream.format("delta") \
    .option("checkpointLocation", f"{volum_path}/checkpoint/bronze") \
    .trigger(availableNow=True) \
    .toTable(f"{bronze_table}"))

df_autoload.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Why foreachBatch method used in streaming?
# MAGIC
# MAGIC **DQX**, is designed to operate on batch DataFrames. **foreachBatch** provides a micro-batch as a standard DataFrame within the function, allowing you to directly apply these rich, batch-based validation rules without needing a separate, streaming-specific library.
# MAGIC
# MAGIC Data quality results often need to be logged to monitoring systems or databases that may not have a native streaming
# MAGIC connector. foreachBatch allows you to use existing batch data writers to send data quality metrics or failed records to any destination

# COMMAND ----------

# MAGIC %md
# MAGIC An **SCD-2** table (**silver_dqx**) in the silver layer is distinguished by the metadata it adds to track changes. For every business entity (e.g., a customer or product), multiple rows can exist in the table, each representing a different **historical version**.
# MAGIC
# MAGIC **Version tracking columns** 
# MAGIC The **silver_dqx** table includes columns to track the validity of each record version.
# MAGIC - **Typical columns are:**
# MAGIC     -   **effective_start_dt**: The timestamp when this version of the record became effective.e
# MAGIC     -   **effective_end_dt**: The timestamp when this version was replaced by a newer version. The current record's end_date is often set to a null value or a distant future date (e.g., 9999-12-31).
# MAGIC     -   **is_current**: A boolean flag (true/false) to easily identify the most recent version of a record.

# COMMAND ----------

# define class BatchProcessHelper

class BatchProcessHelper:

    # Initialize local scop variables

    def __init__(self, volum_path,bronze_table,silver_table,gold_table,quarantine_table):
        self.volum_path = volum_path
        self.bronze_table = bronze_table
        self.silver_table = silver_table
        self.gold_table = gold_table
        self.quarantine_table = quarantine_table

    # define process_batch method to process batch data & apply data quality checks
    def process_batch(self, batch_df, batch_id):
        
        # Initialize DQEngine
        # The **.databrickscfg** file is a configuration file used to store connection and authentication 
        # settings for Databricks  in the form of profiles.
        # It stores connection details, primarily the Databricks host (your workspace URL) and an authentication token 
        # (like a Personal Access Token or PAT).
        # We can define multiple profiles within the same file to manage connections to different Databricks environments, 
        # such as [DEFAULT], [DEV], and [PRODUCTION]. This allows you to easily switch between workspaces 
        # without re-entering credentials.
        dq_engine = DQEngine(WorkspaceClient(config_file=f"{volum_path}/config/.databrickscfg"))
        
        # Validation rules yaml file
        yaml_path = f"{volum_path}/config/rule_config.yaml"

        # Read configuration from yaml file
        with open(yaml_path, 'r') as file:
            rule_set = yaml.safe_load(file)

        # Apply checks and split dataframe into valid and quarantine dataframe
        valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(batch_df, rule_set)
        
        # Write valid and quarantine dataframe to delta table
        valid_count = valid_df.count()
        if valid_count > 0:
            valid_df = valid_df.withColumn("id",col("id").cast("int")) \
                .withColumn("date_joined",F.col("date_joined").cast("date")) \
                .withColumn("effective_start_dt", F.current_timestamp()) \
                .withColumn("effective_end_dt", F.lit(datetime(9999, 12, 31))) \
                .withColumn("is_current", F.lit(True))
                
            if not spark.catalog.tableExists(f"{silver_table}"):
                valid_df.write.format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(f"{silver_table}")
            else:                    
                silver_table_name = DeltaTable.forName(spark, f"{silver_table}")
                
                # Perform the MERGE INTO operation
                silver_table_name.alias("target") \
                    .merge(
                        valid_df.alias("source"),
                        "target.id = source.id"
                    ) \
                    .whenMatchedUpdate(set={"effective_end_dt": F.current_timestamp(),
                                    "is_current": F.lit(False)}) \
                    .whenNotMatchedInsertAll() \
                    .execute()
                    
                valid_df.write.format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(f"{silver_table}")
        
        # Storing Quarantine Dataframe
        quarantine_count = quarantine_df.count()
        if quarantine_count > 0:
            quarantine_df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(f"{quarantine_table}")

# Read from Bronze
# df_bronze = spark.readStream.table("uc_catalog.default.bronze_dqx")
batch_instance = BatchProcessHelper(volum_path,bronze_table,silver_table,gold_table,quarantine_table)

# start bronze streaming and initiate call of process batch function
query = df_autoload.writeStream \
    .foreachBatch(batch_instance.process_batch) \
    .option("checkpointLocation", f"{volum_path}/checkpoint/foreach") \
    .start()


# COMMAND ----------

# MAGIC %md
# MAGIC A gold layer table gold_dqx is designed for BI reporting and aggregates information for further use. Business logic dictates that only the current information is needed for most reports. 

# COMMAND ----------


query = f"""
CREATE OR REPLACE VIEW {gold_table} AS
SELECT
  id, name, age, status, age_group, date_joined, gender, city, country, job_title, is_newsletter_subscriber
FROM {silver_table}
WHERE is_current = TRUE
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_catalog.default.gold_dqx

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_catalog.default.quarantine_dqx

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_catalog.default.silver_dqx

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_catalog.default.bronze_dqx