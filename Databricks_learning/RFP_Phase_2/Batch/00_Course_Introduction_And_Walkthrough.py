# Databricks notebook source
# MAGIC %md
# MAGIC # Getting Started with RFP Batch Load
# MAGIC
# MAGIC - In this course, we’ll explore how batch processing is implemented within the Medallion Architecture using Databricks Autoloader for seamless incremental data ingestion.
# MAGIC
# MAGIC - You'll get a hands-on look at how data flows through the Bronze, Silver, and Gold layers—starting from raw file ingestion to refined, query-ready datasets. We'll focus on using Autoloader’s efficient file discovery and metadata tracking features to automate and optimize incremental batch loads at scale.
# MAGIC
# MAGIC - Let’s dive into how modern data engineering leverages Autoloader + Delta Lake to build scalable, fault-tolerant pipelines — without compromising performance.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###🔑 Key Features Used
# MAGIC **Databricks Autoloader (cloudFiles)**
# MAGIC - Automatically detects and ingests new files from cloud storage.
# MAGIC - Supports schema inference and evolution.
# MAGIC
# MAGIC **Incremental Batch Trigger (availableNow=True)**
# MAGIC - Processes only new data in micro-batches.
# MAGIC - Ideal for scheduled or one-time batch loads.
# MAGIC **Checkpointing** Tracks ingested files to ensure exactly-once processing.
# MAGIC
# MAGIC **Medallion Architecture**
# MAGIC Organizes data into:
# MAGIC - 🟤 Bronze – raw data
# MAGIC - ⚪ Silver – cleaned/enriched
# MAGIC - 🟡 Gold – business-level insights
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Medallion Architecture
# MAGIC
# MAGIC _The Medallion Architecture is a data design pattern that organizes data processing into three layers—Bronze (raw), Silver (cleaned), and Gold (aggregated)—to ensure scalability, quality, and reliability in modern data platforms like Databricks._
# MAGIC
# MAGIC ![](/Workspace/Shared/RFP_Batch_Usecase/Includes/Images/medallion arch..png)
# MAGIC
# MAGIC 🥉 Bronze Layer – Raw Ingestion **:** Stores raw, ingested data from source systems with minimal transformation for auditability and traceability.
# MAGIC
# MAGIC - Captures source data as-is (CSV, JSON, Parquet, streaming feeds)
# MAGIC - Applies minimal transformation; preserves full fidelity and historical context
# MAGIC - Acts as a landing zone for downstream processing
# MAGIC - Key attributes: append-only, auditability, reprocessability
# MAGIC
# MAGIC 🥈 Silver Layer – Cleaned & Enriched : Cleans, deduplicates, and enriches data to create a refined and query-ready dataset.
# MAGIC
# MAGIC - Cleanses and standardizes data (e.g. type-casting, null filtering, deduplication)
# MAGIC - Joins or integrates with lookup/reference data for enterprise views
# MAGIC - Still granular enough for data engineers and data scientists
# MAGIC - Uses Delta Lake transformations to enforce quality and consistency
# MAGIC
# MAGIC
# MAGIC 🥇 Gold Layer – Curated Business Data :  Aggregates and curates data for business reporting, analytics, client usage for business purposes.
# MAGIC
# MAGIC - Produces aggregated, consumption-ready datasets and key metrics
# MAGIC - Supports analytics, dashboards, BI tools, and ML pipelines
# MAGIC - Often modeled in star-schema or denormalized structures for performance
# MAGIC - Final layer for business consumption and decision making

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###📊 Data Lineage Overview: Bronze → Silver → Gold
# MAGIC **🔹 Bronze Layer (Raw Ingested Data)**
# MAGIC
# MAGIC - Ingests raw files from source systems or data volumes.
# MAGIC - Minimal transformation — primarily schema inference and metadata enrichment (e.g., load timestamps, file paths).
# MAGIC - Acts as the immutable source of truth for traceability and reprocessing.
# MAGIC
# MAGIC **🔘 Silver Layer (Cleaned & Transformed Data)**
# MAGIC
# MAGIC - Applies data cleaning, validation, and standardization.
# MAGIC - Removes unnecessary metadata and formats values for consistency (e.g., data type casting, null handling).
# MAGIC - Provides a refined, query-ready dataset for analysts and downstream processing.
# MAGIC
# MAGIC **🏅 Gold Layer (Business-Level Aggregates)**
# MAGIC
# MAGIC - Performs business logic, aggregations, and joins to create domain-specific summary tables or dashboards.
# MAGIC - Optimized for reporting, machine learning, or decision support.
# MAGIC - Represents the most trusted and high-value form of the data.
# MAGIC
# MAGIC **🔁 End-to-End Flow**
# MAGIC
# MAGIC - Data flows through a structured and scalable pipeline, improving quality and relevance at each layer.
# MAGIC - Follows the medallion architecture pattern — ensuring modularity, performance, and maintainability across stages.
# MAGIC
# MAGIC ![](/Workspace/Shared/RFP_Batch_Usecase/Includes/Images/Lineage)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚀 What Is Databricks Auto Loader?
# MAGIC
# MAGIC - A scalable ingestion engine that incrementally reads new files from cloud storage (S3, ADLS, GCS, DBFS) using the Structured Streaming source cloudFiles 
# MAGIC - Supports a wide range of formats: JSON, CSV, Parquet, Avro, XML, ORC, TEXT, and BINARYFILE 
# MAGIC - Automatically handles file detection, schema inference & evolution, and checkpointing using a RocksDB key-value store for fault tolerance and exactly-once processing 
# MAGIC
# MAGIC ### 🛠 How Auto Loader Works
# MAGIC **File Detection**
# MAGIC - Uses Directory Listing (polls the folder) or Cloud File Notifications (event-driven via Event Grid) to detect new files 
# MAGIC
# MAGIC **Schema Management**
# MAGIC - Infers schema on first ingestion and supports schema evolution when new columns are added, using options like addNewColumns or rescueSchema 
# MAGIC
# MAGIC **Checkpointing**
# MAGIC - Stores processed file metadata and offsets in the checkpoint location using RocksDB, enabling reliable exactly-once semantics even after job failures or restarts 
# MAGIC
# MAGIC **Writing to Delta Lake**
# MAGIC - Streams ingested records directly into Delta tables, enabling ACID transactions and integration with further ETL or analytics layers.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Run Setup
# MAGIC **Pre requisite** : User should have **'Create Catalog'** Permission and UC enabled cluster.
# MAGIC
# MAGIC ### 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Here we get started with:
# MAGIC
# MAGIC ### Full Loads
# MAGIC
# MAGIC [1.0_RFP_Batch_Full_Load_Bronze]($./1.0_RFP_Batch_Full_Load_Bronze) **🔧 What the Code Does**
# MAGIC
# MAGIC - Loads environment variables from Env_setup (e.g., catalog, schema names, paths).
# MAGIC - load_files_bronze(): Generates data for a given table.
# MAGIC - Loop over all Bronze tables: Loads data files and Writes them to the Delta table.
# MAGIC
# MAGIC [1.1_RFP_Batch_Full_Load_Silver]($./1.1_RFP_Batch_Full_Load_Silver) **🔧 What the Code Does**
# MAGIC
# MAGIC - Loops through Bronze-to-Silver table mappings.
# MAGIC - Reads data from Bronze layer 
# MAGIC - Applies table-specific transformations
# MAGIC - Writes cleaned/transformed data to the corresponding Silver Delta table in append mode.
# MAGIC
# MAGIC [1.2_RFP_Batch_Full_Load_Gold]($./1.2_RFP_Batch_Full_Load_Gold) **🔧 What the Code Does**
# MAGIC
# MAGIC - Loops over Silver-to-Gold table mappings (silver_to_gold_mapping).
# MAGIC - Reads data from Silver tables.
# MAGIC - Applies table-specific aggregations to create business-level summaries.
# MAGIC - Writes the aggregated results to Gold Delta tables, using overwrite mode.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Loads
# MAGIC
# MAGIC [1.3_RFP_Batch_Inc_Load_Bronze_AutoLoader]($./1.3_RFP_Batch_Inc_Load_Bronze_AutoLoader)
# MAGIC **🔧 What the Code Does**
# MAGIC - Batch load: Generates and saves new JSON files for each Bronze table.
# MAGIC - Incremental load : Uses Autoloader to stream new JSON files with schema.
# MAGIC - Extracts and Writes data incrementally to Bronze Delta tables with checkpointing.
# MAGIC - Trigger set to (availableNow=True) and then stop.
# MAGIC - Runs incremental load for all Bronze tables in a loop.
# MAGIC
# MAGIC [1.4_RFP_Batch_Inc_Load_Silver]($./1.4_RFP_Batch_Inc_Load_Silver)
# MAGIC **🔧 What the Code Does**
# MAGIC - Gets the latest ingestion timestamp from the Silver table.
# MAGIC - Filters Bronze data to keep only records newer than the latest Silver ingestion timestamp (incremental data).
# MAGIC - Uses Delta Lake merge operation to upsert incremental Bronze data into Silver table based on merge keys.
# MAGIC - Updates matching records, inserts new records.
# MAGIC - Applies table-specific data transformations (casting dates, filling nulls, cleaning fields).
# MAGIC - Merges cleaned data into Silver table using merge_to_silver().
# MAGIC
# MAGIC [1.5_RFP_Batch_Inc_Load_Gold]($./1.5_RFP_Batch_Inc_Load_Gold)
# MAGIC **🔧 What the Code Does**
# MAGIC - Loops through Silver → Gold table mappings.
# MAGIC - Reads data from each Silver table.
# MAGIC - Performs aggregations based on table type:
# MAGIC - In both cases, aggregate counts, avg cost, payer coverage, and total spend.
# MAGIC - Writes the result to the Gold Delta table in overwrite mode.
# MAGIC