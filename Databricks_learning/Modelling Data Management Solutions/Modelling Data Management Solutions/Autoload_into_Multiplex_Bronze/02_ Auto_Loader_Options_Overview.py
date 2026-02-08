# Databricks notebook source
# MAGIC %md
# MAGIC ##2. Auto Loader Options Overview
# MAGIC ###  What are Auto Loader Options?
# MAGIC
# MAGIC Auto Loader options control how Databricks ingests files incrementally from cloud object storage.  
# MAGIC They define behavior for schema handling, file discovery, filtering, throughput, and fault tolerance.
# MAGIC
# MAGIC #### Schema & Evolution:
# MAGIC - `cloudFiles.schemaLocation`: track schema history (⭐ required)
# MAGIC - `cloudFiles.inferColumnTypes`: infer non-string types
# MAGIC - `cloudFiles.schemaHints`: enforce specific column types
# MAGIC - `cloudFiles.schemaEvolutionMode`: add new columns or rescue data automatically
# MAGIC - Sink `.option("mergeSchema", "true")`: evolve the target Delta table
# MAGIC
# MAGIC #### Rescued Data:
# MAGIC - `rescuedDataColumn`: capture unexpected or malformed fields safely
# MAGIC - Ensures ingestion continues without breaking on schema drift
# MAGIC
# MAGIC #### Discovery & Backfill:
# MAGIC - `cloudFiles.includeExistingFiles`: one-time backfill of historical files
# MAGIC - `cloudFiles.backfillInterval`: periodically recheck directories in listing mode
# MAGIC - `cloudFiles.useNotifications`: event-driven discovery with cloud notifications
# MAGIC
# MAGIC #### File Filtering:
# MAGIC - `pathGlobFilter`: filter by path or extension (e.g., `*.json`)
# MAGIC - `fileNamePattern`: regex-like filename matching
# MAGIC - `modifiedAfter`: process only files newer than a timestamp
# MAGIC
# MAGIC #### Throughput & Behavior:
# MAGIC - `cloudFiles.maxFilesPerTrigger`: throttle number of files per micro-batch
# MAGIC - `cloudFiles.allowOverwrites`: reprocess files if their contents change
# MAGIC - `ignoreCorruptFiles`: skip corrupted files instead of failing the stream
# MAGIC