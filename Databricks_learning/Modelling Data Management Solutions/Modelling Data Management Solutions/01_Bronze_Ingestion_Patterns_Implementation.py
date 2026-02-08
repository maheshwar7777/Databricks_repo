# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Patterns in Databricks
# MAGIC
# MAGIC **1. Bronze Layer Overview**
# MAGIC -     Responsibilities
# MAGIC -     Input Sources (Batch, Streaming)
# MAGIC -     File Formats (CSV, JSON, Parquet, etc.)
# MAGIC
# MAGIC **2. Ingestion Patterns in Bronze Layer**
# MAGIC -     Pattern 1: Streaming File Ingestion (Auto Loader)
# MAGIC -     Pattern 2: Data ingestion using COPY INTO command

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze Layer Overview

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### What is the Bronze Layer?
# MAGIC
# MAGIC The Bronze layer is the **landing zone for raw data**. It is the first stop for data as it arrives from various sources.
# MAGIC
# MAGIC #### Responsibilities:
# MAGIC - Capture raw data with minimal transformations
# MAGIC - Store metadata (ingestion time, source info)
# MAGIC - Support schema evolution and auditing
# MAGIC - Act as a replayable source for downstream layers
# MAGIC
# MAGIC #### Common Input Sources:
# MAGIC - Files from cloud storage (e.g., Azure Data Lake, AWS S3)
# MAGIC - Kafka / Event Hubs
# MAGIC - Databases (via CDC or snapshot exports)
# MAGIC
# MAGIC #### Common File Formats:
# MAGIC - CSV
# MAGIC - JSON
# MAGIC - Parquet
# MAGIC - Avro
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingestion Patterns in Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1: USE  AUTO LOADER

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auto Loader Overview:
# MAGIC Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage without any additional setup.It is designed to handle large-scale data ingestion with minimal setup and maintenance.
# MAGIC
# MAGIC ### How Auto Loader Works:
# MAGIC Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage. It provides a Structured Streaming source called cloudFiles. Given an input directory path on the cloud file storage, the cloudFiles source automatically processes new files as they arrive, with the option of also processing existing files in that directory. Auto Loader has support for both Python and SQL in Lakeflow Declarative Pipelines.
# MAGIC - You can use Auto Loader to process billions of files to migrate or backfill a table. Auto Loader scales to support near real-time ingestion of millions of files per hour.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Supported Auto Loader Sources
# MAGIC Auto Loader can load data files from the following sources:
# MAGIC - Amazon S3 (s3://)
# MAGIC - Azure Data Lake Storage (ADLS, abfss://)
# MAGIC - Google Cloud Storage (GCS, gs://)
# MAGIC - Azure Blob Storage (wasbs://)
# MAGIC - Databricks File System (DBFS, dbfs:/).
# MAGIC - Auto Loader can ingest JSON, CSV, XML, PARQUET, AVRO, ORC, TEXT, and BINARYFILE file formats.

# COMMAND ----------

# MAGIC %md
# MAGIC ### How does Auto Loader track ingestion progress:
# MAGIC As files are discovered, their metadata is persisted in a scalable key-value store (RocksDB) in the checkpoint location of your Auto Loader pipeline. This key-value store ensures that data is processed exactly once.
# MAGIC In case of failures, Auto Loader can resume from where it left off by information stored in the checkpoint location and continue to provide exactly-once guarantees when writing data into Delta Lake. You don't need to maintain or manage any state yourself to achieve fault tolerance or exactly-once semantics.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Above Documentation Source:
# MAGIC (https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)

# COMMAND ----------

# MAGIC %md
# MAGIC ### AUTO LOADER IMPLEMENTATION

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up Catalog, Schema and Volume

# COMMAND ----------

# MAGIC %sql
# MAGIC ---create catalog
# MAGIC create catalog if not exists autoloader_catalog;
# MAGIC use catalog autoloader_catalog;
# MAGIC
# MAGIC ---create schema
# MAGIC create schema if not exists autoloader_schema;
# MAGIC use schema autoloader_schema;
# MAGIC
# MAGIC ---craete volume under this autoloader_catalog ->autoloader_schema
# MAGIC create volume if not exists autoloader_catalog.autoloader_schema.autoloader_volume
# MAGIC COMMENT 'This is a volume for autoloader'
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create new directory in volume for data landing

# COMMAND ----------

#create new directory on volume location for landing the data
dbutils.fs.mkdirs("/Volumes/autoloader_catalog/autoloader_schema/autoloader_volume/landing_data/autoloader_input")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upload data on landing zone(Volume)

# COMMAND ----------

# MAGIC %md
# MAGIC I have uploaded my data files on volume landing zone

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Checkpoint location in volume

# COMMAND ----------

# create checkpoint directory on volume location
dbutils.fs.mkdirs("/Volumes/autoloader_catalog/autoloader_schema/autoloader_volume/landing_data/checkpoint")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auto Loader File Detection Modes
# MAGIC -  Read files using auto loader with checkpoint
# MAGIC -  And schema location for schema inference is (volume location)
# MAGIC -  checkpoint directory is (volume location)"/Volumes/autoloader_catalog/autoloader_schema/autoloader_volume/landing_data/checkpoint"
# MAGIC -  File Detection modes:-
# MAGIC -  1. Directory listing ( **uses API call to detect new files**)
# MAGIC -  2. File Notification (**uses Notification and queue services**- requires elevated cloud permission for setup)
# MAGIC
# MAGIC RocksDB is used for Metadata:
# MAGIC - Auto loader identifies files and ensures that their metadata is saved in a scalable key-value store (**RocksDB**) in its pipeline's checkpoint location
# MAGIC
# MAGIC Note:- 
# MAGIC Notification service would put a file event with metadata in the Queue for Auto Loader to subscribe and consume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Initialization & Stream Read
# MAGIC This section:
# MAGIC - Defines the **catalog**, **schema**, and **table** for the Bronze layer  
# MAGIC - Configures **checkpoint** and **schemaLocation** paths inside a UC Volume (`autoloader_volume`)  
# MAGIC - Uses **Auto Loader (`cloudFiles`)** to continuously read JSON files from the sample dataset  
# MAGIC - Enables **metadata capture** (e.g., file path, modification time) for traceability  
# MAGIC - Runs `display(raw_df)` to trigger and preview the live stream

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Data Using Auto Loader

# COMMAND ----------

# DBTITLE 1,Auto Loader File Detection Modes
# Read files using auto loader with checkpoint
# And schema location for schema inference is (volume location)
# checkpoint directory is (volume location)"/Volumes/autoloader_catalog/autoloader_schema/autoloader_volume/landing_data/checkpoint"
# 1. File Detection modes
# - Directory listing ( uses API call to detect new files)
# - File Notification (uses Notification and queue services- requires elevated cloud permission for setup)
# 2. Schema Location in Auto Loader:- It is used to handle Schema Evolution (changes in schema) automatically.
# 3. Schema Hints in autoloader:- Allows to define schema for certain specific columns(allows Only that column which we need)

df= (
    spark
    .readStream
    .format("cloudFiles")                  # Autoloader
    .option("cloudFiles.format", "csv")    # Format of file
    .option("pathGlobFilter", "*.csv")     #Global filter
    .option("header", "true")              # To handle header
    #.option("cloudFiles.useNotifications", "true")   # To handle file notification
    #.option("cloudFiles.schemaHints", "id string, name string, date string") -> allows only required column.
    .option("cloudFiles.schemaLocation", "/Volumes/autoloader_catalog/autoloader_schema/autoloader_volume/landing_data/checkpoint/schema/")      # To handle schema evolution
    option("coudFiles.schemaEvolutionMode", "rescue")  # To rescue schema evolution
    .load("/Volumes/autoloader_catalog/autoloader_schema/autoloader_volume/landing_data/autoloader_input")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Stream to Bronze Delta Table
# MAGIC This step:
# MAGIC - Defines the **target Bronze table** in Unity Catalog (`catalog.schema.table`)  
# MAGIC - Writes the streaming DataFrame (`bronze_df`) into Delta with:
# MAGIC   - **checkpointing** (stored in your UC Volume for exactly-once guarantees)  
# MAGIC   - **schema evolution** enabled (`mergeSchema=true`)  
# MAGIC   - **append mode** to accumulate raw events  
# MAGIC - Uses `.trigger(availableNow=True)` to process all available files once, then stop (ideal for SQL Warehouses / restricted clusters)  
# MAGIC - Automatically creates the table if it doesn’t already exist

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Data into Broze/Target table (Delta Table)

# COMMAND ----------

# Write data into delta table (Bronze table)
from pyspark.sql.functions import *
(
  df
  .writeStream
  .format("delta")
  .option("checkpointLocation", "/Volumes/autoloader_catalog/autoloader_schema/autoloader_volume/landing_data/checkpoint/bronze")
  .outputMode("append")
  .trigger(availableNow=True)
  .toTable("autoloader_catalog.autoloader_schema.autoloader_bronze")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Varify data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from autoloader_catalog.autoloader_schema.autoloader_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC So, once the new data will uploaded on the source location then auto loader automatically detect and load into target table i,e. bronze table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2: USE COPY INTO COMMAND 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Case 1: Ingest Data in Bronze Table from DBFS Using COPY INTO command

# COMMAND ----------

# DBTITLE 1,Create Sample Data on DBFS Location
# Create a simple DataFrame
data = [
    (1, "Alice", "2022-01-01"),
    (2, "Bob", "2022-01-02"),
    (3, "Charlie", "2022-01-03")
]
columns = ["id", "name", "date"]

df = spark.createDataFrame(data, columns)

# Save as CSV to DBFS location
dbfs_path = "dbfs:/Filestore/mnt/sample_data/input_csv_data"

# Save as CSV with header
df.write.option("header", True).mode("overwrite").csv(dbfs_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Catalog & Schema 

# COMMAND ----------

# DBTITLE 1,Create Catalog, Schema
# MAGIC %sql
# MAGIC create catalog if not exists sample_bronze_catalog;
# MAGIC use catalog sample_bronze_catalog;
# MAGIC create schema if not exists sample_bronze_schema;
# MAGIC use schema sample_bronze_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Unity Catalog Delta Table

# COMMAND ----------

# DBTITLE 1,Create Table
# MAGIC %sql
# MAGIC -- Create a managed Delta table
# MAGIC CREATE table sample_bronze_catalog.sample_bronze_schema.sample_uc_bronze_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use COPY INTO to Load Data

# COMMAND ----------

# DBTITLE 1,Use COPY INTO to ingest data into Bronze Table
# MAGIC %sql
# MAGIC --Use COPY INTO to load data into the target/bronze table
# MAGIC COPY INTO sample_bronze_catalog.sample_bronze_schema.sample_uc_bronze_table
# MAGIC FROM "dbfs:/Filestore/mnt/sample_data/input_csv_data"
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = '*.csv'
# MAGIC FORMAT_OPTIONS (
# MAGIC     'mergeSchema' = 'true',
# MAGIC     'header' = 'true'
# MAGIC     )
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Result:

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample_bronze_catalog.sample_bronze_schema.sample_uc_bronze_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Case 2: Ingest Data from Volume into Bronze Table using COPY INTO 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Volume

# COMMAND ----------

# DBTITLE 1,Create Volume Location
# MAGIC %sql
# MAGIC ---Create new volume LANDING under sample_bronze_catalog -> sample_bronze_schema
# MAGIC create volume if not exists sample_bronze_catalog.sample_bronze_schema.landing
# MAGIC COMMENT 'This is a landing managed volume'
# MAGIC     
# MAGIC ---Move the bronze table to the volume

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Input folder under the Volume

# COMMAND ----------

# create new folder called input under landing volume
dbutils.fs.mkdirs("/Volumes/sample_bronze_catalog/sample_bronze_schema/landing/input")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Target/Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC ---create bronze table to get data from the volume
# MAGIC CREATE TABLE IF NOT EXISTS sample_bronze_catalog.sample_bronze_schema.landing_bronze_table;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### USE COPY INTO COMMAND

# COMMAND ----------

# DBTITLE 1,Use COPY INTO
# MAGIC %sql
# MAGIC ---use copy into command to ingest data from volume location to bronze table
# MAGIC COPY INTO sample_bronze_catalog.sample_bronze_schema.landing_bronze_table
# MAGIC FROM "/Volumes/sample_bronze_catalog/sample_bronze_schema/landing/input"
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = '*.csv'
# MAGIC FORMAT_OPTIONS (
# MAGIC     'mergeSchema' = 'true',      ---Source data option
# MAGIC     'header' = 'true'            ---Source data option
# MAGIC     )
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');    ---Target table data option

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify Data:-

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample_bronze_catalog.sample_bronze_schema.landing_bronze_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Case 3: Ingest Data From External Location Using COPY INTO

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1:- Upload files on the External location(S3/ADLS)

# COMMAND ----------

# MAGIC %md
# MAGIC I have uploaded my csv file on AWS S3 location:-
# MAGIC - Path is:- s3://healthcare-data-databricks/bronze-ingestion-pattern/Car_Sales_Data.csv

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2:- Create Bronze/Target Table in Unity Catalog

# COMMAND ----------

# DBTITLE 1,Bronze Table
# MAGIC %sql
# MAGIC ---create bronze/target table to ingest data from external/s3 location
# MAGIC CREATE TABLE IF NOT EXISTS sample_bronze_catalog.sample_bronze_schema.s3_data_bronze_table;

# COMMAND ----------

# MAGIC %md
# MAGIC #### USE COPY INTO COMMAND

# COMMAND ----------

# MAGIC %sql
# MAGIC ---use copy into command to ingest data from volume location to bronze table
# MAGIC COPY INTO sample_bronze_catalog.sample_bronze_schema.s3_data_bronze_table
# MAGIC FROM "s3://healthcare-data-databricks/bronze-ingestion-pattern"
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = '*.csv'
# MAGIC FORMAT_OPTIONS (
# MAGIC     'mergeSchema' = 'true',      ---Source data option
# MAGIC     'header' = 'true'            ---Source data option
# MAGIC     )
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');    ---Target table data option

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify Data:-

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample_bronze_catalog.sample_bronze_schema.s3_data_bronze_table;

# COMMAND ----------

# MAGIC %md
# MAGIC #### COPY INTO maintains metadata in Databricks:

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended sample_bronze_catalog.sample_bronze_schema.s3_data_bronze_table;

# COMMAND ----------

# MAGIC %md
# MAGIC - We can see the metadata of this data on this above location:-
# MAGIC s3://databricksmetadata/metadata/ca176069-539d-4275-99ca-7ee14e5f5d96/tables/154cbdfd-a61c-442d-929a-ebd3eae9cff5
# MAGIC - When we will go and see on this location then we will be able to find out there is a folder name which is copy into log which is createc when we excute the copy into command. So at this location copy into maintains its metadata.
# MAGIC
# MAGIC Note:-
# MAGIC - COPY INTO maintain metadata at table level. So, if a new table is created data will get loaded again. 