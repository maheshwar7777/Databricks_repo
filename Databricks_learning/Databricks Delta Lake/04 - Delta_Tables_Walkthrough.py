# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delta Lake Fundamentals Training
# MAGIC
# MAGIC This comprehensive training covers essential Delta Lake concepts and operations for data engineers.
# MAGIC
# MAGIC #### Training Modules:
# MAGIC - **Delta Table Creation** - SQL and Python approaches
# MAGIC - **Managed vs External Tables** - Storage and governance differences  
# MAGIC - **Transaction Logs** - Understanding Delta's ACID guarantees
# MAGIC - **Performance Optimization** - OPTIMIZE and Z-ORDER operations
# MAGIC - **Table Conversion** - Migrating Parquet tables to Delta format
# MAGIC
# MAGIC #### Prerequisites:
# MAGIC - Unity Catalog enabled workspace
# MAGIC - CREATE CATALOG and CREATE SCHEMA permissions
# MAGIC - Basic SQL and Python knowledge

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training Agenda
# MAGIC
# MAGIC 1. **Setup and Configuration** [config.py]($./Includes/config.py)
# MAGIC 2. **Generate Sample Data** [Data-Generator]($./Includes/Data-Generator)
# MAGIC 3. **Core Concepts Lab** [01-Delta_Tables_Core_Concepts]($./01-Delta_Tables_Core_Concepts)
# MAGIC 4. **Managed vs External Tables** [02-Managed_vs_External_Tables]($./02-Managed_vs_External_Tables)
# MAGIC 5. **Transaction Logs Deep Dive** [03-Delta_Transaction_Logs]($./03-Delta_Transaction_Logs)
# MAGIC 6. **Performance Optimization** [04-Optimize_and_ZOrder]($./04-Optimize_and_ZOrder)
# MAGIC 7. **Table Conversion** [05-Parquet_to_Delta_Conversion]($./05-Parquet_to_Delta_Conversion)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Setup

# COMMAND ----------

from Includes.config import * 
from Includes.delta_utility import *

# COMMAND ----------

# Widgets for configuration
dbutils.widgets.dropdown("data_size", "Medium", ["Small", "Medium", "Large"], "Dataset Size")
dbutils.widgets.dropdown("enable_cdf", "true", ["true", "false"], "Enable Change Data Feed")
dbutils.widgets.dropdown("auto_optimize", "true", ["true", "false"], "Enable Auto Optimize")

# COMMAND ----------

# Generate sample data
dbutils.notebook.run("./Includes/data_generator", 300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Overview

# COMMAND ----------

print_delta_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Path
# MAGIC
# MAGIC Navigate through the notebooks in order to build your Delta Lake expertise:
# MAGIC
# MAGIC ### Module 1: Core Concepts
# MAGIC **[Delta Tables Core Concepts]($./05 - Delta_Tables_Core_Concepts)**
# MAGIC - Creating tables with SQL DDL and Python DataFrame API
# MAGIC - Basic CRUD operations with ACID guarantees
# MAGIC - Schema enforcement and evolution
# MAGIC
# MAGIC ### Module 2: Table Types
# MAGIC **[Managed vs External Tables]($./06 - Managed_vs_External_Tables)**
# MAGIC - Understanding storage models
# MAGIC - Lifecycle management differences
# MAGIC - Best practices for each approach
# MAGIC
# MAGIC ### Module 3: 
# MAGIC **[Update Delte Merge]($./07 - Delta_Lake_Update_Delete_Merge)**
# MAGIC - Master all MERGE INTO scenarios: MATCHED, NOT MATCHED, NOT MATCHED BY SOURCE
# MAGIC - Understand conditional merge operations and complex logic
# MAGIC - Learn schema evolution with MERGE operations
# MAGIC
# MAGIC ### Module 4: 
# MAGIC **[History Vacuum TimeTravel]($./08 - History_Vacuum_TimeTravel)**
# MAGIC - Understand table operation history and how to retrieve it
# MAGIC - Safely reclaim storage space using VACUUM (including LITE mode)
# MAGIC - Use Delta Lake Time Travel for audits, restores, and reproducible analytics
# MAGIC
# MAGIC ### Module 5: Architecture
# MAGIC **[Delta Transaction Logs]($./07 - Delta_Transaction_Logs)**
# MAGIC - Transaction log structure and format
# MAGIC - How ACID properties are maintained
# MAGIC - Checkpoint files and performance
# MAGIC
# MAGIC ### Module 6: Performance
# MAGIC **[Optimize and Z-Order]($./08 - Optimize_and_ZOrder)**
# MAGIC - File compaction strategies
# MAGIC - Multi-dimensional clustering
# MAGIC - Performance monitoring and tuning
# MAGIC
# MAGIC ### Module 7: Migration
# MAGIC **[Parquet to Delta Conversion]($./05-Parquet_to_Delta_Conversion)**
# MAGIC - Converting existing Parquet tables
# MAGIC - In-place vs copy-based conversion
# MAGIC - Validation and testing strategies

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Delta Lake Benefits
# MAGIC
# MAGIC ### ACID Transactions
# MAGIC - **Atomicity**: Operations complete fully or not at all
# MAGIC - **Consistency**: Data integrity rules are maintained
# MAGIC - **Isolation**: Concurrent operations don't interfere
# MAGIC - **Durability**: Committed changes persist
# MAGIC
# MAGIC ### Schema Management
# MAGIC - **Schema Enforcement**: Prevents bad data from entering
# MAGIC - **Schema Evolution**: Safely modify table structure
# MAGIC - **Data Quality**: Built-in validation and constraints
# MAGIC
# MAGIC ### Performance Features
# MAGIC - **File Compaction**: Optimize small file problems
# MAGIC - **Data Skipping**: Skip irrelevant files during queries
# MAGIC - **Z-Ordering**: Multi-dimensional clustering
# MAGIC - **Caching**: Intelligent data caching

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ready to Start?
# MAGIC
# MAGIC Begin with Module 1 to establish the fundamentals:
# MAGIC
# MAGIC **[Start with Core Concepts →]($./05 - Delta_Tables_Core_Concepts)**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Run this when training is complete:

# COMMAND ----------

cleanup_delta_demo()