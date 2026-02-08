# Databricks notebook source
# MAGIC %md
# MAGIC **Overview of DLT Integration with DQX:** 
# MAGIC
# MAGIC Delta Live Tables uses expectations as a mechanism to define data quality constraints directly within the pipeline. The DQX framework extends this capability by allowing you to automatically generate DLT-compatible expectations from data profiles.

# COMMAND ----------

# MAGIC %md
# MAGIC **Overview:** The purpose of this implementation is how the DQX framework integrates with Databricks Delta Live Tables (DLT) to enable seamless data quality enforcement in DLT pipelines using Auto Loader, SCD type2.
# MAGIC
# MAGIC Showcases modern Lakehouse best practices Medallion architecture, streaming ingestion, data quality, and SCD2, Demonstrates Lakeflow Declarative Pipelines features Streaming tables, declarative transformations, and data quality integration.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Installs the Databricks Labs DQX library, which provides advanced data quality and profiling tools, Enables the use of DQX for data quality checks in the pipeline.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx
# MAGIC %pip install databricks-labs-dqx==0.8.0

# COMMAND ----------

# MAGIC %md
# MAGIC Restarts the Python process to ensure newly installed libraries are available, required after %pip install to make sure the DQX library is loaded and restart after installing new libraries in a notebook session.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Imports all necessary libraries for Lakeflow Declarative Pipelines, Spark, DQX, configuration and Prepares the environment for data processing, quality checks, and logging.

# COMMAND ----------

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import explode, col
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx import check_funcs
import yaml
import logging

# COMMAND ----------

# MAGIC %md
# MAGIC Setting up logging for the notebook, Useful for debugging and tracking pipeline execution.

# COMMAND ----------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC Initializes the DQX Data Quality Engine with workspace credentials, Required to apply data quality checks using DQX in the pipeline.

# COMMAND ----------

from databricks.labs.dqx.engine import WorkspaceFileChecksStorageConfig

dq_engine = DQEngine(WorkspaceClient(config_file="//Workspace/Users/kiran.olipilli@impetus.com/.databrickscfg"))

# COMMAND ----------

# MAGIC %md
# MAGIC Defines a Lakeflow Declarative Pipelines streaming table (bronze layer) that ingests raw CSV data using Auto Loader, here bronze table is the raw landing zone for incoming data, enabling incremental and scalable ingestion.

# COMMAND ----------

from pyspark.sql.types import *

bronze_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("gender", StringType(), True)
])

@dlt.table(
    name="uc_catalog.learning.bronze_dlt"
)
def load_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .schema(bronze_schema)
        .load("/Volumes/uc_catalog/learning/kiran_dqx_vol/dlt_pipeline/input_data/")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC Defining the silver table, which reads from the bronze table, Cleans and normalizes data (gender normalization, type casting), Applies data quality checks using DQX, loaded from a YAML config. Here the silver layer is for cleaned, validated data ready for further processing.

# COMMAND ----------

import dlt, yaml
from pyspark.sql.functions import col, when

@dlt.table(
    name="uc_catalog.learning.silver_dlt",
    comment="Cleaned data with gender normalised and DQ checks applied"
)
def validate_data():
    yaml_path = "/Volumes/uc_catalog/learning/kiran_dqx_vol/dlt_pipeline/config/dqx_quality_rules.yaml"
    with open(yaml_path, "r") as f:
        checks = yaml.safe_load(f)
    if isinstance(checks, dict) and "checks" in checks:
        checks = checks["checks"]

    input_df = dlt.read_stream("uc_catalog.learning.bronze_dlt")
    from pyspark.sql.functions import lower

    cleaned_df = (
        input_df
        .withColumn("signup_date", col("signup_date").cast("string"))
        .withColumn("age", col("age").cast("int"))
        .withColumn(
            "gender",
            when(lower(col("gender")).isin("female", "femail", "f"), "female")
            .when(lower(col("gender")).isin("male", "m"), "male")
            .otherwise("unknown")
        )
    )
    valid_df, _ = dq_engine.apply_checks_by_metadata_and_split(cleaned_df, checks)
    return valid_df

# COMMAND ----------

# MAGIC %md
# MAGIC Here Implementing SCD2 (Slowly Changing Dimension Type 2) logic Creates a view over the silver table, defines a new streaming table for SCD2 and Uses dlt.apply_changes to track changes over time for storing history. SCD2 is essential for historical tracking of changes like user profile updates.

# COMMAND ----------

@dlt.view
def silver_source():
    return spark.readStream.table("uc_catalog.learning.silver_dlt")

dlt.create_streaming_table(
    name="uc_catalog.learning.silver_scd2"
)

import dlt
from pyspark.sql.functions import col

dlt.apply_changes(
    target = "uc_catalog.learning.silver_scd2",
    source = "silver_source",
    keys   = ["id"],
    sequence_by = col("signup_date"),
    stored_as_scd_type = 2,
    track_history_column_list = [
        "name", "email", "age", "signup_date", "gender"
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC Defining the gold table, which reads from the SCD2 silver table, Applies final transformations and type casting for analytics. 

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="uc_catalog.learning.gold_dlt",
    comment="Cleaned and transformed user data from SCD2 silver"
)
def gold_table():
    df = spark.readStream.table("uc_catalog.learning.silver_scd2")
    return (
        df
        .withColumn("signup_date_clean", col("signup_date").cast("string"))
        .withColumn("gender_clean", col("gender").cast("string"))
        .withColumn("age_clean", col("age").cast("int"))
    )