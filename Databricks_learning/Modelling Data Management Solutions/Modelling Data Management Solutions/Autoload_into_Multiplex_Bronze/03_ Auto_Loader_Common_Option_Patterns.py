# Databricks notebook source
# MAGIC %md
# MAGIC #  Auto Loader Common Option Patterns Lab
# MAGIC
# MAGIC This interactive notebook demonstrates **common Auto Loader option patterns**.  
# MAGIC Each step explains the concept, shows the options, and runs a demo ingestion.
# MAGIC
# MAGIC We’ll cover:
# MAGIC 1. Minimal Setup  
# MAGIC 2. Strong Typing with Hints  
# MAGIC 3. Schema Evolution  
# MAGIC 4. Rescued Data Column  
# MAGIC 5. File Filtering & Backfill  
# MAGIC
# MAGIC Dataset: `/databricks-datasets/structured-streaming/events/` (demo JSON files)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **-#01_Setup — Initialize Catalog, Schema, and Paths
# MAGIC This section:**
# MAGIC - Defines the **catalog**, **schema**, and **table** names for the Bronze layer  
# MAGIC - Points to the **sample JSON dataset**  
# MAGIC - Sets the **checkpoint** and **schemaLocation** paths  
# MAGIC - Creates (if needed) and switches to the target schema
# MAGIC

# COMMAND ----------

# 01_Setup
catalog = "streaming_events_06"              # <- change if needed (Unity Catalog)
schema  = "bronze_demo"       # <- change if needed
table   = "events_bronze"     # target Bronze Delta table

source_path     = "/databricks-datasets/structured-streaming/events/"  # sample JSON events
checkpoint_path = f"/tmp/{schema}/{table}/_checkpoints"
schema_loc_path = f"/tmp/{schema}/{table}/_schema"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")


# COMMAND ----------

# MAGIC %md
# MAGIC
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

# MAGIC %md
# MAGIC ## 2️⃣ Strong Typing with Hints
# MAGIC
# MAGIC **Goal:** Get better types & enforce specific ones.  
# MAGIC - `cloudFiles.inferColumnTypes = true`  
# MAGIC - `cloudFiles.schemaHints = "time DOUBLE, json STRING"`

# COMMAND ----------

# MAGIC %md
# MAGIC # Auto Loader Options — Key Features
# MAGIC
# MAGIC **1. Strong Typing with Hints**  
# MAGIC - Ensures columns get the right data types instead of defaulting to `STRING`.  
# MAGIC   - **Example:**  
# MAGIC     - `cloudFiles.inferColumnTypes = true`  
# MAGIC     - `.option("cloudFiles.schemaHints", "time DOUBLE, json STRING")`
# MAGIC
# MAGIC **2. Schema Evolution**  
# MAGIC - Lets new columns be added automatically when they appear in the data.  
# MAGIC   - **Example:**  
# MAGIC     - `.option("cloudFiles.schemaEvolutionMode", "addNewColumns")`
# MAGIC
# MAGIC **3. Rescued Data Column**  
# MAGIC - Captures unexpected fields or type mismatches instead of dropping rows.  
# MAGIC   - **Example:**  
# MAGIC     - `.option("rescuedDataColumn", "_rescued_data")`
# MAGIC
# MAGIC **4. File Filtering & Backfill**  
# MAGIC - Ingest only certain files or reprocess older ones.  
# MAGIC   - **Examples:**  
# MAGIC     - `.option("pathGlobFilter", "*.json")`  
# MAGIC     - `.option("modifiedAfter", "2010-01-01 00:00:00 UTC")`

# COMMAND ----------

# ----- Auto Loader Demo with Options + Refinements + Metadata -----

from pyspark.sql.functions import col, to_timestamp, current_timestamp

# 1) Read from built-in dataset with Auto Loader
raw_df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", schema_loc_path)
         .option("cloudFiles.includeExistingFiles", "true")
         .option("cloudFiles.inferColumnTypes", "true") 
         .option("cloudFiles.schemaHints", "time DOUBLE, json STRING")   # Strong typing
         .option("pathGlobFilter", "*.json")                            # File filtering
         .option("modifiedAfter", "2010-01-01 00:00:00 UTC")            # Backfill
         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")     # Schema evolution
         .option("includeMetadata", "true")                             # Metadata
         .load(source_path)
)

display(raw_df)   # show raw with rescued_data + metadata

# 2) Refinement: add event_time_utc, drop rescued data
refined_df = (
    raw_df
      .withColumn("event_time_utc", to_timestamp(col("time").cast("double")))
      .drop("_rescued_data")
)

display(refined_df)   # cleaner stream

# 3) Bronze enrichment: add ingest_time + source_file
bronze_df = (
    raw_df
      .withColumn("ingest_time", current_timestamp())
      .withColumn("source_file", col("_metadata.file_path"))
)

display(bronze_df)    # Bronze-style enriched stream


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Notes**
# MAGIC - `cloudFiles.schemaLocation` stores schema history for **inference & evolution**.  
# MAGIC - `includeExistingFiles` allows one-time backfill, then incremental loads.  
# MAGIC - `cloudFiles.inferColumnTypes` performs an extra pass to infer data types. :contentReference[oaicite:6]{index=6}
# MAGIC

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
        .option("mergeSchema", "true")                  # evolve if schema changes
        .outputMode("append")
        .queryName("multiplex_bronze_ingest")
        # .trigger(processingTime="10 seconds")
        .trigger(availableNow=True)
        .toTable(target_fullname)                       # auto-creates table if missing
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## IGNORE DOWN PORTIONS `****`

# COMMAND ----------

# # 0) Setup — point at your bucket and make unique run paths
# import time, json

# catalog = "streaming_events_06"     # use your catalog
# schema  = "bronze_demo"             # use your schema
# evo_tbl = f"{catalog}.{schema}.events_bronze_evolution"    # target UC table

# BASE = "s3://healthcare-data-databricks/json_test/landing_json"
# run_id = str(int(time.time()*1000))

# src = f"{BASE.rstrip('/')}/evo_demo_{run_id}"             # source folder to drop JSON
# cp  = f"{BASE.rstrip('/')}/_state/evo/{run_id}/_cp"       # checkpoint location
# sl  = f"{BASE.rstrip('/')}/_state/evo/{run_id}/_schema"   # schemaLocation
# print("SRC:", src, "\nCP :", cp, "\nSL :", sl)


# COMMAND ----------

# # ▶ Rescued Data Column — capture schema drift; separate state
# import time
# run_id = str(int(time.time()*1000))

# resc_checkpoint = f"/Volumes/{catalog}/{schema}/_lab_state/{table}/resc_{run_id}/_cp"
# resc_schema_loc = f"/Volumes/{catalog}/{schema}/_lab_state/{table}/resc_{run_id}/_schema"
# resc_table      = f"{catalog}.{schema}.events_bronze_rescued"

# spark.sql(f"DROP TABLE IF EXISTS {resc_table}")

# df_resc = (
#     spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .option("cloudFiles.schemaLocation", resc_schema_loc)
#         .option("cloudFiles.includeExistingFiles", "true")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option("rescuedDataColumn", "_rescued_data")
#         .load(source_path)
# )

# (df_resc.writeStream
#     .format("delta")
#     .option("checkpointLocation", resc_checkpoint)
#     .outputMode("append")
#     .trigger(availableNow=True)
#     .toTable(resc_table))

# display(spark.sql(f"""
#   SELECT action,
#          CAST(time AS STRING)             AS time,
#          CAST(_rescued_data AS STRING)    AS rescued_json,
#          CASE WHEN _rescued_data IS NULL THEN 0 ELSE 1 END AS has_rescued
#   FROM {resc_table}
#   LIMIT 20
# """))


# COMMAND ----------

# # 1) Seed two tiny NDJSON files (2 rows each)
# #    - First file: {action, time}
# #    - Second file: adds {device} to trigger schema evolution

# from pyspark.sql import Row

# t0 = int(time.time())
# ndjson_1 = "\n".join([
#   json.dumps({"action":"Open",  "time": t0}),
#   json.dumps({"action":"Close", "time": t0+1})
# ]) + "\n"

# ndjson_2 = "\n".join([
#   json.dumps({"action":"Open",  "time": t0+2, "device":"mobile"}),
#   json.dumps({"action":"Close", "time": t0+3, "device":"web"})
# ]) + "\n"

# # write into your S3 prefix
# dbutils.fs.mkdirs(src)
# dbutils.fs.put(f"{src}/part_01.json", ndjson_1, overwrite=True)
# dbutils.fs.put(f"{src}/part_02.json", ndjson_2, overwrite=True)

# # sanity check (batch read count > 0)
# print("Batch sanity rows:", spark.read.format("json").load(src).count())
# display(dbutils.fs.ls(src))