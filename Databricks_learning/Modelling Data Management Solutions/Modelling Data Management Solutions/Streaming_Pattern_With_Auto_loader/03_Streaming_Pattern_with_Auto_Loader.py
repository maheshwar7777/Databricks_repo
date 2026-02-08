# Databricks notebook source
# MAGIC %md
# MAGIC # 00_Streaming_From_Multiplex_Bronze
# MAGIC
# MAGIC This lab walks through a step-by-step process to ingest streaming JSON files into a **Bronze** table using **Structured Streaming + Auto Loader**.  
# MAGIC You’ll set up paths, read a stream, do light transforms, write to Delta with checkpoints, and monitor the query.  
# MAGIC (See Databricks “Run your first Structured Streaming workload” and concepts. ) 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Introduction
# MAGIC
# MAGIC ### What is Structured Streaming?
# MAGIC A Spark engine for **near real-time, incremental** processing with **exactly-once guarantees**, using familiar DataFrame/SQL APIs.
# MAGIC
# MAGIC ### Role in Streaming Tutorial
# MAGIC We’ll continuously ingest JSON files from object storage using **Auto Loader (`cloudFiles`)**, leveraging **schema inference**, **schema evolution**, **checkpointing**, and **triggered micro-batches** to land raw data in **Bronze**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Structured Streaming Overview
# MAGIC
# MAGIC **Responsibilities**
# MAGIC - Process data continuously as it arrives (near real-time).
# MAGIC - Maintain **fault tolerance** via checkpoints; support **exactly-once** semantics.
# MAGIC - Use **output modes**, **triggers**, and **sinks** (Delta tables, etc.)
# MAGIC
# MAGIC **Common Input Sources**
# MAGIC - Incremental files in cloud storage (S3/ADLS/GCS) via **Auto Loader**
# MAGIC
# MAGIC **Common File Formats**
# MAGIC - JSON, CSV, Parquet, Avro. (Auto Loader supports schema inference & evolution.){index=5}
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Catalog and Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC --  1) Create the catalog ()
# MAGIC CREATE CATALOG IF NOT EXISTS streaming_events_06;
# MAGIC
# MAGIC --  2) Switch into that catalog
# MAGIC USE CATALOG streaming_events_06;
# MAGIC
# MAGIC --  3) Create the schema inside the catalog ()
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze_demo;
# MAGIC
# MAGIC --  4) Switch into the schema 
# MAGIC USE SCHEMA bronze_demo;
# MAGIC
# MAGIC -- ✅ 5) Create the volume inside the current catalog/schema
# MAGIC CREATE VOLUME IF NOT EXISTS _lab_state
# MAGIC COMMENT 'Streaming checkpoints & schemaLocation for tutorials';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - #  01_Setup — Initialize Catalog, Schema, and Paths
# MAGIC This section:
# MAGIC - Defines the **catalog**, **schema**, and **table** names for the Bronze layer  
# MAGIC - Points to the **sample JSON dataset**  
# MAGIC - Sets the **checkpoint** and **schemaLocation** paths  
# MAGIC - Creates (if needed) and switches to the target schema
# MAGIC

# COMMAND ----------

# 01_Setup
catalog = "streaming_events_06"              
schema  = "bronze_demo"      
table   = "events_bronze"     # target Bronze Delta table

source_path     = "/databricks-datasets/structured-streaming/events/"  # sample JSON events
checkpoint_path = f"/tmp/{schema}/{table}/_checkpoints"
schema_loc_path = f"/tmp/{schema}/{table}/_schema"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Step 1 — Setup Initialization & Stream Read
# MAGIC
# MAGIC This section:
# MAGIC - Defines the **catalog**, **schema**, and **table** for the Bronze layer  
# MAGIC - Configures **checkpoint** and **schemaLocation** paths inside a UC Volume (`_lab_state`)  
# MAGIC - Uses **Auto Loader (`cloudFiles`)** to continuously read JSON files from the sample dataset  
# MAGIC - Enables **metadata capture** (e.g., file path, modification time) for traceability  
# MAGIC - Runs `display(raw_df)` to trigger and preview the live stream
# MAGIC

# COMMAND ----------

# ----- Setup Initialisation-----
catalog = "streaming_events_06"
schema  = "bronze_demo"
table   = "events_bronze"

source_path = "/databricks-datasets/structured-streaming/events/"  # built-in sample dataset (read-only)

# Use the UC Volume for all streaming state:
checkpoint_path = f"/Volumes/{catalog}/{schema}/_lab_state/{table}/checkpoints"
schema_loc_path = f"/Volumes/{catalog}/{schema}/_lab_state/{table}/schema"

# (Optional) make sure the folders exist
dbutils.fs.mkdirs(checkpoint_path)
dbutils.fs.mkdirs(schema_loc_path)

# ----- Read JSON files as a stream with Auto Loader -----
raw_df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", schema_loc_path)   
         .option("cloudFiles.inferColumnTypes", "true")
         .option("cloudFiles.includeExistingFiles", "true")
         .option("includeMetadata", "true")  
         .load(source_path)                                     
)

display(raw_df)  # Triggers the stream in notebooks


# COMMAND ----------

# MAGIC %md
# MAGIC **Notes**
# MAGIC - `cloudFiles.schemaLocation` stores schema history for **inference & evolution**.  
# MAGIC - `includeExistingFiles` allows one-time backfill, then incremental loads.  
# MAGIC - `cloudFiles.inferColumnTypes` performs an extra pass to infer data types. :contentReference[oaicite:6]{index=6}
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Step 2 — Refine Stream Data
# MAGIC
# MAGIC This step:
# MAGIC - Converts the numeric **`time`** field into a proper **UTC timestamp** (`event_time_utc`)  
# MAGIC - Drops the **`_rescued_data`** column since it’s empty in our demo dataset  
# MAGIC - Prepares a cleaner stream DataFrame (`refined_df`) for downstream use
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

refined_df = (
    raw_df
      .withColumn("event_time_utc", to_timestamp(col("time").cast("double")))
      .drop("_rescued_data")   
)

display(refined_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #  Step 3 — Enrich Bronze Data
# MAGIC
# MAGIC This step:
# MAGIC - Adds an **`ingest_time`** column with the current timestamp (when the row was processed)  
# MAGIC - Extracts the **source file path** from Auto Loader’s `_metadata.file_path` field for lineage  
# MAGIC - Produces the **Bronze DataFrame (`bronze_df`)** ready to be written into the Bronze Delta table
# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

bronze_df = (
    raw_df
      .withColumn("ingest_time", current_timestamp())
      .withColumn("source_file", col("_metadata.file_path"))
)

display(bronze_df)


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Step 4 — Write Stream to Bronze Delta Table
# MAGIC
# MAGIC This step:
# MAGIC - Defines the **target Bronze table** in Unity Catalog (`catalog.schema.table`)  
# MAGIC - Writes the streaming DataFrame (`bronze_df`) into Delta with:
# MAGIC   - **checkpointing** (stored in your UC Volume for exactly-once guarantees)  
# MAGIC   - **schema evolution** enabled (`mergeSchema=true`)  
# MAGIC   - **append mode** to accumulate raw events  
# MAGIC - Uses `.trigger(availableNow=True)` to process all available files once, then stop (ideal for SQL Warehouses / restricted clusters)  
# MAGIC - Automatically creates the table if it doesn’t already exist
# MAGIC

# COMMAND ----------

# 05_Write_to_Delta (Bronze) — auto-create table from DataFrame schema
target_fullname = f"{catalog}.{schema}.{table}"

stream = (
    bronze_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)  # /Volumes/... path
        .option("mergeSchema", "true")                 
        .outputMode("append")
        .queryName("multiplex_bronze_ingest")
        # .trigger(processingTime="10 seconds")
        .trigger(availableNow=True)
        .toTable(target_fullname)                       # auto-creates table if missing
)


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Verify the Bronze Table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_in_bronze
# MAGIC FROM streaming_events_06.bronze_demo.events_bronze;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Monitor & Control
# MAGIC
# MAGIC - In the **Spark UI → Structured Streaming** tab, find your query by **queryName** to inspect **input rates, batch durations, state**, etc.  
# MAGIC - You can stop the stream programmatically.
# MAGIC

# COMMAND ----------

# 07_List_and_Stop_Stream (as needed)
for s in spark.streams.active:
    print(s.name, s.id, s.status)

# Example: stop by name 
# next(s for s in spark.streams.active if s.name == "multiplex_bronze_ingest").stop()


# COMMAND ----------

# 08_Enable_Rescued_Data_Column (Optional)
raw_df_rescued = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_loc_path)
        .option("rescuedDataColumn", "_rescued_data")  # rename if desired
        .load(source_path)
)
display(raw_df_rescued.selectExpr("*", "_rescued_data"))


# COMMAND ----------

# MAGIC %md
# MAGIC # 🔄 Reset Demo State (Bronze Streaming)
# MAGIC
# MAGIC This step:
# MAGIC - **Stops** the running stream (if active)
# MAGIC - **Drops** the Bronze table (`catalog.schema.table`)
# MAGIC - **Clears** the **checkpoint** and **schemaLocation** folders (so Auto Loader reprocesses files)
# MAGIC - Prints a short status so you know it’s clean
# MAGIC
# MAGIC > ⚠️ Only use this for demos. In production, **do not** delete checkpoints.
# MAGIC

# COMMAND ----------

# Reset Demo State — safe to run multiple times

# 0) Ensure required vars exist (fall back to your defaults if missing)
if "catalog" not in locals(): catalog = "streaming_events_06"
if "schema" not in locals():  schema  = "bronze_demo"
if "table" not in locals():   table   = "events_bronze"
if "checkpoint_path" not in locals():
    checkpoint_path = f"/Volumes/{catalog}/{schema}/_lab_state/{table}/checkpoints"
if "schema_loc_path" not in locals():
    schema_loc_path = f"/Volumes/{catalog}/{schema}/_lab_state/{table}/schema"

target_fullname = f"{catalog}.{schema}.{table}"

# 1) Stop the stream if it's running
stopped = False
for s in spark.streams.active:
    if s.name in ("multiplex_bronze_ingest", "ss_multiplex_bronze", "ss_multiplex_bronze_path"):
        try:
            s.stop()
            stopped = True
        except Exception as e:
            print(f"Warning: Could not stop stream {s.name}: {e}")

# 2) Drop the Bronze table
spark.sql(f"DROP TABLE IF EXISTS `{target_fullname}`")

# 3) Clear checkpoint & schemaLocation
import time
def _rm(path):
    try:
        dbutils.fs.rm(path, recurse=True)
        return True
    except Exception as e:
        print(f"Warning: Could not remove {path}: {e}")
        return False

cp_removed  = _rm(checkpoint_path)
sch_removed = _rm(schema_loc_path)

# 4) (Optional) Recreate empty folders (nice for UI browsing)
try:
    dbutils.fs.mkdirs(checkpoint_path)
    dbutils.fs.mkdirs(schema_loc_path)
except Exception as e:
    print(f"Warning: Could not recreate folders: {e}")

# 5) Status
print("=== Reset Complete ===")
print(f"Stream stopped:         {stopped}")
print(f"Dropped table:          {target_fullname}")
print(f"Checkpoint cleared:     {cp_removed} -> {checkpoint_path}")
print(f"SchemaLocation cleared: {sch_removed} -> {schema_loc_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Production Tips (Optional)
# MAGIC
# MAGIC - Prefer **queryName** and monitor in the UI; ensure **unique checkpoint** per writer.  
# MAGIC - Tune triggers (for example, `processingTime` vs. `availableNow`) and **backpressure** options.  
# MAGIC - Consider **notifications** vs. listing for file discovery as appropriate. :contentReference[oaicite:10]{index=10}
# MAGIC

# COMMAND ----------

# 09_Stop_and_Cleanup (Optional)
for s in spark.streams.active:
    if s.name in ("multiplex_bronze_ingest",):
        s.stop()

# spark.sql(f"DROP TABLE IF EXISTS {target_fullname}")
# dbutils.fs.rm(checkpoint_path, True)
# dbutils.fs.rm(schema_loc_path, True)