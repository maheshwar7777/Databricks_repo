# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Python for Delta Live Tables
# MAGIC
# MAGIC We'll explore the contents of this notebook to better understand the syntax used by Delta Live Tables.
# MAGIC
# MAGIC This notebook uses Python to declare Delta Live Tables that together implement a simple multi-hop architecture based on example dataset loaded by default into workspaces.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Define tables with Delta Live Tables
# MAGIC * Use Python to incrementally ingest raw data with Auto Loader

# COMMAND ----------

from Includes.config import * 
import dlt
from pyspark.sql.functions import *
source_catalog = spark.conf.get("catalog_name")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Declare Bronze Layer Tables
# MAGIC
# MAGIC Below we declare two tables implementing the bronze layer. This represents data in its rawest form

# COMMAND ----------

# MAGIC %md
# MAGIC ## patients_raw
# MAGIC
# MAGIC **`patients_raw`** ingests CSV data incrementally from the example dataset found in */raw_schema/sample_data/raw_data/*.
# MAGIC
# MAGIC Incremental processing via [Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html) (which uses the same processing model as Structured Streaming), requires the use of `read_stream()` from `spark.readStream` or `cloud_files()` in SQL. The Python DLT API supports Auto Loader by using `spark.readStream.format("cloudFiles")`.
# MAGIC
# MAGIC This example enables schema inference with the option **`cloudFiles.inferColumnTypes`** set to **`true`**.
# MAGIC
# MAGIC The `@dlt.table` decorator below includes additional metadata like a comment and table properties, which are stored in the catalog for exploration.

# COMMAND ----------

@dlt.table(
    name="ptients_raw",
    comment="The raw sales orders, ingested from  /raw_schema/sample_data/raw_data/.",
    table_properties={
        "quality": "bronze",
        "source": "csv_autoloader"
    }
)
def ptients_raw():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")           # Expecting CSV files
            .option("cloudFiles.inferSchema", "true")     # Infer schema automatically
            .option("header", "true")                     # First row contains column names
            .option("cloudFiles.schemaLocation",f"/Volumes/{source_catalog}/{SCHEMA_PATH}ptients_raw")  # Where to store inferred schema
            .load(f"/Volumes/{source_catalog}/{BASE_PATH}patients/")
    )

# COMMAND ----------

@dlt.table(
    name="encounter_raw",
    comment="The raw sales orders, ingested from  /raw_schema/sample_data/raw_data/.",
    table_properties={
        "quality": "bronze",
        "source": "csv_autoloader"
    }
)
def encounter_raw():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")           # Expecting CSV files
            .option("cloudFiles.inferSchema", "true")     # Infer schema automatically
            .option("header", "true")                     # First row contains column names
            .option("cloudFiles.schemaLocation",f"/Volumes/{source_catalog}/{SCHEMA_PATH}encounter_raw")  # Where to store inferred schema
            .load(f"/Volumes/{source_catalog}/{BASE_PATH}encounters/")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Declare Silver Layer Tables
# MAGIC
# MAGIC Now we declare tables implementing the silver layer. This layer represents a refined copy of data from the bronze layer, with the intention of optimizing downstream applications. At this level we apply operations like data cleansing and enrichment.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##silver_patient
# MAGIC
# MAGIC This is our first Silver table, which will contain cleaned data of raw_patinets 
# MAGIC and applies **quality control** by rejecting records with a null `Id`.
# MAGIC
# MAGIC ###Quality Control
# MAGIC
# MAGIC We use the `@dlt.expect_or_drop` decorator to define a constraint that drops records with `NULL` order numbers.  
# MAGIC DLT will collect metrics for such violations.
# MAGIC
# MAGIC

# COMMAND ----------


@dlt.table(
    name="silver_patient",
    comment="The cleaned sales orders with valid order_number(s) and partitioned by order_datetime.",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_birthdate", "birthdate IS NOT NULL")
@dlt.expect_or_drop("passport_not_null", "passport IS NOT NULL")
def silver_patients():
    return dlt.read_stream("ptients_raw")

# COMMAND ----------

@dlt.table(
    name="silver_encounters",
    comment="Filtered encounters with valid patient ID and cost"
)
@dlt.expect_or_drop("valid_patient", "patient IS NOT NULL")
@dlt.expect_or_drop("valid_cost", "base_encounter_cost > 0")
@dlt.expect_or_drop("total_claim_cost_range", "total_claim_cost BETWEEN 0 AND 20000")
def silver_encounters():
    return dlt.read_stream("encounter_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Declare Gold Table
# MAGIC
# MAGIC At the most refined level of the architecture, we declare a table delivering an aggregation with business value, in this case we collect patient encounters enriched with patient demographics.

# COMMAND ----------

@dlt.table(
    name="gold_patient_encounters",
    comment="Patient encounters enriched with patient demographics"
)
def gold_patient_encounters():
    encounters = dlt.read_stream("silver_encounters").withColumn("start_ts", to_timestamp("start")).withWatermark("start_ts", "30 days")
    patients = dlt.read("silver_patient")

    return (
        encounters.alias("e")
        .join(patients.alias("p"), col("e.patient") == col("p.id"), "left")
        .select(
            col("e.id").alias("encounter_id"),
            "e.start", "e.stop", "e.encounterclass", "e.description",
            "e.base_encounter_cost", "e.total_claim_cost",
            "p.first", "p.last", "p.gender", "p.race", "p.birthdate", "p.city", "p.state"
        )
    )