# Databricks notebook source
# MAGIC %md
# MAGIC #### Module 5: Delta Transaction Logs Deep Dive
# MAGIC
# MAGIC This module explores the architecture and mechanics of Delta Lake's transaction log system, which enables ACID guarantees and time travel capabilities.
# MAGIC
# MAGIC #### Learning Objectives
# MAGIC - Understand the structure and purpose of Delta transaction logs
# MAGIC - Explore how ACID properties are maintained
# MAGIC

# COMMAND ----------

from Includes.config import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transaction Log Architecture
# MAGIC
# MAGIC #### The `_delta_log` Directory
# MAGIC ```
# MAGIC table_location/
# MAGIC ├── _delta_log/
# MAGIC │   ├── 00000000000000000000.json    # Version 0
# MAGIC │   ├── 00000000000000000001.json    # Version 1
# MAGIC │   ├── 00000000000000000002.json    # Version 2
# MAGIC │   ├── ...
# MAGIC │   └── 00000000000000000010.checkpoint.parquet  # Checkpoint
# MAGIC ├── part-00000-xxx.parquet          # Data files
# MAGIC ├── part-00001-xxx.parquet
# MAGIC └── ...
# MAGIC ```
# MAGIC
# MAGIC #### Key Components
# MAGIC - **JSON Files**: Individual transaction records
# MAGIC - **Checkpoint Files**: Aggregated state snapshots  
# MAGIC - **Data Files**: Actual table data in Parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 1: Creating a Table to Examine Logs

# COMMAND ----------

# Create a table with several operations to generate transaction history
spark.sql("""
CREATE OR REPLACE TABLE delta_demo.transaction_example (
  id BIGINT NOT NULL,
  name STRING,
  category STRING,
  price DECIMAL(10,2),
  created_date DATE
  
)
USING DELTA
PARTITIONED BY (category)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported'
)
""")

print("Table created - this generates transaction log version 0")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 2: Generating Transaction History

# COMMAND ----------

# Operation 1: Initial data insert
spark.sql("""
INSERT INTO delta_demo.transaction_example VALUES
(1, 'Laptop', 'Electronics', 999.99, '2024-01-15'),
(2, 'Book', 'Education', 29.99, '2024-01-15'),
(3, 'Shirt', 'Clothing', 49.99, '2024-01-15')
""")
print("Version 1: Initial data insert")

# COMMAND ----------

#### Operation 2: Update operation
spark.sql("""
UPDATE delta_demo.transaction_example 
SET price = price * 0.9
WHERE category = 'Electronics'
""")
print("Version 2: Price update for Electronics")

# COMMAND ----------

# Operation 3: Additional inserts
spark.sql("""
INSERT INTO delta_demo.transaction_example VALUES
(4, 'Tablet', 'Electronics', 299.99, '2024-01-16'),
(5, 'Novel', 'Education', 15.99, '2024-01-16')
""")
print("Version 3: Additional products inserted")

# COMMAND ----------

# Operation 4: Delete operation
spark.sql("""
DELETE FROM delta_demo.transaction_example 
WHERE price < 20
""")
print("Version 4: Delete low-price items")

# COMMAND ----------

# Operation 5: Schema evolution
spark.sql("""
ALTER TABLE delta_demo.transaction_example 
ADD COLUMNS (
  discount_percent INT,
  is_featured BOOLEAN 
)
""")
print("Version 5: Schema evolution - added new columns")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 3: Examining Transaction Log Files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Table Location and Log Directory

# COMMAND ----------

# This will ALWAYS work regardless of storage location
display(spark.sql("DESCRIBE HISTORY delta_demo.transaction_example"))

# Or using Delta Lake API
from delta.tables import DeltaTable
delta_table = DeltaTable.forName(spark, "delta_demo.transaction_example")
display(delta_table.history())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Transaction Log Contents

# COMMAND ----------

# MAGIC %md
# MAGIC **Common Delta Lake Actions:**
# MAGIC
# MAGIC - **`commitInfo`**: Metadata about the operation (user, timestamp, operation type)
# MAGIC - **`protocol`**: Delta Lake protocol version information
# MAGIC - **`metaData`**: Table schema, partition columns, and configuration
# MAGIC - **`add`**: Files added to the table
# MAGIC - **`remove`**: Files removed from the table (due to updates/deletes)
# MAGIC - **`txn`**: Transaction identifiers for streaming applications
# MAGIC - **`cdc`**: Change data capture records (if enabled)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Key Takeaways
# MAGIC
# MAGIC 1. **Transaction Logs Enable ACID Properties**
# MAGIC    - Each operation creates a new version
# MAGIC    - Atomic commits ensure consistency
# MAGIC    - Isolation prevents read anomalies
# MAGIC    - Durability guaranteed through persistent logs
# MAGIC
# MAGIC 2. **Log Structure and Components**
# MAGIC    - JSON files for each transaction version
# MAGIC    - Checkpoint files for performance optimization
# MAGIC    - Action types track all table changes
# MAGIC    - Commit info provides operation metadata
# MAGIC
# MAGIC
# MAGIC
# MAGIC Next: **[Optimize and Z-Order →]($./04-Optimize_and_ZOrder)**