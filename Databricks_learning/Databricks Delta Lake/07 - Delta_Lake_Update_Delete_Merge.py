# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3: MERGE INTO SQL Operations in Databricks
# MAGIC
# MAGIC This module provides comprehensive coverage of Delta Lake's MERGE INTO operations, including all scenario types and best practices for data synchronization and upsert operations.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Master all MERGE INTO scenarios: MATCHED, NOT MATCHED, NOT MATCHED BY SOURCE
# MAGIC - Understand conditional merge operations and complex logic
# MAGIC - Learn schema evolution with MERGE operations
# MAGIC - Implement performance optimization techniques
# MAGIC - Apply best practices for production workloads
# MAGIC - Monitor and troubleshoot MERGE operations
# MAGIC
# MAGIC ## Reference Documentation
# MAGIC **Databricks Official Documentation:** [MERGE INTO SQL Language Manual](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into)

# COMMAND ----------

from Includes.config import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ## General MERGE INTO Syntax in Delta Lake
# MAGIC
# MAGIC ```
# MAGIC [ common_table_expression ]
# MAGIC   MERGE [ WITH SCHEMA EVOLUTION ] INTO target_table_name [target_alias]
# MAGIC      USING source_table_reference [source_alias]
# MAGIC      ON merge_condition
# MAGIC      { WHEN MATCHED [ AND matched_condition ] THEN matched_action |
# MAGIC        WHEN NOT MATCHED [BY TARGET] [ AND not_matched_condition ] THEN not_matched_action |
# MAGIC        WHEN NOT MATCHED BY SOURCE [ AND not_matched_by_source_condition ] THEN not_matched_by_source_action } [...]
# MAGIC
# MAGIC matched_action
# MAGIC  { DELETE |
# MAGIC    UPDATE SET * |
# MAGIC    UPDATE SET { column = { expr | DEFAULT } } [, ...] }
# MAGIC
# MAGIC not_matched_action
# MAGIC  { INSERT * |
# MAGIC    INSERT (column1 [, ...] ) VALUES ( expr | DEFAULT ] [, ...] )
# MAGIC
# MAGIC not_matched_by_source_action
# MAGIC  { DELETE |
# MAGIC    UPDATE SET { column = { expr | DEFAULT } } [, ...] }
# MAGIC ```
# MAGIC
# MAGIC - `WHEN MATCHED`: Handle rows where source and target match
# MAGIC - `WHEN NOT MATCHED BY TARGET`: Insert rows present in source but not in target
# MAGIC - `WHEN NOT MATCHED BY SOURCE`: Handle rows present in target but absent in source (for deletes or updates)
# MAGIC - `WITH SCHEMA EVOLUTION`: Automatically update target schema to match source schema during merge

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Demo Environment

# COMMAND ----------

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")

# Create database for our examples
spark.sql("CREATE DATABASE IF NOT EXISTS merge_demo")
spark.sql("USE merge_demo")

print("Demo environment created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: WHEN MATCHED Scenarios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1.1: WHEN MATCHED - DELETE All Matched Rows
# MAGIC
# MAGIC **Use Case:** Remove all records from target that have matching records in source

# COMMAND ----------

# Clean slate for first example
spark.sql("DROP TABLE IF EXISTS target")
spark.sql("DROP TABLE IF EXISTS source")

# Create target table
spark.sql("""
CREATE TABLE target (
  key INT,
  value STRING,
  updated_at TIMESTAMP,
  marked_for_deletion BOOLEAN
) USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

# Insert sample data
spark.sql("""
INSERT INTO target VALUES
  (1, 'value1', '2024-01-01 10:00:00', false),
  (2, 'value2', '2024-01-02 10:00:00', false),
  (3, 'value3', '2024-01-03 10:00:00', true),
  (4, 'value4', '2024-01-04 10:00:00', false),
  (5, 'value5', '2024-01-05 10:00:00', false)
""")

# Create source table
spark.sql("""
CREATE TABLE source (
  key INT,
  value STRING,
  updated_at TIMESTAMP,
  created_at TIMESTAMP,
  marked_for_deletion BOOLEAN
) USING DELTA
""")

# Insert source data (keys 1, 3, 5 will match)
spark.sql("""
INSERT INTO source VALUES
  (1, 'new_value1', '2024-01-06 10:00:00', '2024-01-06 09:00:00', false),
  (3, 'new_value3', '2024-01-06 10:00:00', '2024-01-06 09:00:00', false),
  (5, 'new_value5', '2024-01-06 10:00:00', '2024-01-06 09:00:00', false)
""")

print("Tables created and populated for WHEN MATCHED DELETE example")

# COMMAND ----------

# Show data before merge
print("=== DATA BEFORE MERGE ===")
print("\nTARGET TABLE:")
spark.sql("SELECT * FROM target ORDER BY key").show()

print("SOURCE TABLE:")
spark.sql("SELECT key, value, updated_at, marked_for_deletion FROM source ORDER BY key").show()

# COMMAND ----------

# Execute MERGE: Delete all target rows that have a match in the source table
start_time = time.time()

spark.sql("""
MERGE INTO target USING source
ON target.key = source.key
WHEN MATCHED THEN DELETE
""")

end_time = time.time()
print(f"MERGE completed in {end_time - start_time:.3f} seconds")

# COMMAND ----------

# Show results
print("=== DATA AFTER MERGE (DELETE MATCHED) ===")
spark.sql("SELECT * FROM target ORDER BY key").show()

print(" Analysis: Keys 1, 3, 5 were deleted because they had matches in source table")
print(" Remaining: Keys 2, 4 (no matches in source)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1.2: WHEN MATCHED - Conditional UPDATE with UPDATE SET *

# COMMAND ----------

# Reset data for conditional update example
spark.sql("DROP TABLE target")
spark.sql("""
CREATE TABLE target (
  key INT,
  value STRING,
  updated_at TIMESTAMP,
  marked_for_deletion BOOLEAN
) USING DELTA
""")

spark.sql("""
INSERT INTO target VALUES
  (1, 'old_value1', '2024-01-01 10:00:00', false),
  (2, 'old_value2', '2024-01-02 10:00:00', false),
  (3, 'old_value3', '2024-01-07 10:00:00', true),  -- newer timestamp
  (4, 'old_value4', '2024-01-04 10:00:00', false)
""")

# Create source table
spark.sql("""
CREATE OR REPLACE TABLE source (
  key INT,
  value STRING,
  updated_at TIMESTAMP,
  created_at TIMESTAMP,
  marked_for_deletion BOOLEAN
) USING DELTA
""")

# Insert source data (keys 1, 3, 5 will match)
spark.sql("""
INSERT INTO source VALUES
  (1, 'new_value1', '2024-01-06 10:00:00', '2024-01-06 09:00:00', false),
  (3, 'new_value3', '2024-01-06 10:00:00', '2024-01-06 09:00:00', false),
  (5, 'new_value5', '2024-01-06 10:00:00', '2024-01-06 09:00:00', false)
""")

print("Target table reset for conditional UPDATE example")

# COMMAND ----------

# Show before state
print("=== DATA BEFORE CONDITIONAL UPDATE ===")
print("\nTARGET TABLE:")
spark.sql("SELECT * FROM target ORDER BY key").show()

print("SOURCE TABLE:")
spark.sql("SELECT key, value, updated_at, marked_for_deletion FROM source ORDER BY key").show()

# COMMAND ----------

# Conditionally update target rows using UPDATE SET *
spark.sql("""
MERGE INTO target USING source
ON target.key = source.key
WHEN MATCHED AND target.updated_at < source.updated_at THEN UPDATE SET *
""")

print("Conditional UPDATE completed")

# COMMAND ----------

# Show results
print("=== DATA AFTER CONDITIONAL UPDATE ===")
spark.sql("SELECT * FROM target ORDER BY key").show()

print(" Analysis:")
print("   - Key 1: Updated (target timestamp older than source)")
print("   - Key 3: NOT updated (target timestamp newer than source)")
print("   - Keys 2, 4: Unchanged (no match in source)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1.3: WHEN MATCHED - Multiple MATCHED Clauses

# COMMAND ----------

# Reset for multiple matched clauses example
spark.sql("DROP TABLE target")
spark.sql("""
CREATE TABLE target (
  key INT,
  value STRING,
  updated_at TIMESTAMP,
  marked_for_deletion BOOLEAN
) USING DELTA
""")

spark.sql("""
INSERT INTO target VALUES
  (1, 'value1', '2024-01-01 10:00:00', false),
  (2, 'value2', '2024-01-02 10:00:00', true),  -- marked for deletion
  (3, 'value3', '2024-01-03 10:00:00', false),
  (4, 'value4', '2024-01-04 10:00:00', true),  -- marked for deletion
  (5, 'value5', '2024-01-05 10:00:00', false)
""")

# Create source table
spark.sql("""
CREATE OR REPLACE TABLE source (
  key INT,
  value STRING,
  updated_at TIMESTAMP,
  created_at TIMESTAMP,
  marked_for_deletion BOOLEAN
) USING DELTA
""")

# Insert source data (keys 1, 3, 5 will match)
spark.sql("""
INSERT INTO source VALUES
  (1, 'value1', '2024-01-06 10:00:00', '2024-01-06 09:00:00', false),
  (2, 'value2', '2024-01-06 10:00:00', '2024-01-06 09:00:00', false),
  (3, 'value3', '2024-01-06 10:00:00', '2024-01-06 09:00:00', false),
  (4, 'value2', '2024-01-06 10:00:00', '2024-01-06 09:00:00', false),
  (5, 'value5', '2024-01-06 10:00:00', '2024-01-06 09:00:00', false)
""")

print("Target table reset for multiple MATCHED clauses example")

# COMMAND ----------

# Show before state
print("=== DATA BEFORE MULTIPLE MATCHED CLAUSES ===")
print("\nTARGET TABLE:")
spark.sql("SELECT * FROM target ORDER BY key").show()

print("SOURCE TABLE:")
spark.sql("SELECT key, value, updated_at, marked_for_deletion FROM source ORDER BY key").show()

# COMMAND ----------

# Multiple MATCHED clauses: delete if marked, otherwise update specific columns
spark.sql("""
MERGE INTO target USING source
ON target.key = source.key
WHEN MATCHED AND target.marked_for_deletion THEN DELETE
WHEN MATCHED THEN UPDATE SET 
  target.updated_at = source.updated_at, 
  target.value = 'UPDATED_' || target.value
""")

print("Multiple MATCHED clauses operation completed")

# COMMAND ----------

# Show results
print("=== DATA AFTER MULTIPLE MATCHED CLAUSES ===")
spark.sql("SELECT * FROM target ORDER BY key").show()

print(" Analysis:")
print("   - Key 1: Updated with new timestamp and modified value")
print("   - Key 2: DELETED (matched AND marked_for_deletion = true)")
print("   - Key 3: Updated with new timestamp and modified value") 
print("   - Key 4: DELETED (matched AND marked_for_deletion = true)")
print("   - Key 5: Updated with new timestamp and modified value")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: WHEN NOT MATCHED [BY TARGET] Scenarios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2.1: WHEN NOT MATCHED - INSERT All Unmatched Rows

# COMMAND ----------

# Reset tables for INSERT examples
spark.sql("DROP TABLE IF EXISTS target")
spark.sql("DROP TABLE IF EXISTS source")

spark.sql("""
CREATE TABLE target (
  key INT,
  value STRING,
  created_at TIMESTAMP
) USING DELTA
""")

spark.sql("""
INSERT INTO target VALUES
  (1, 'existing1', '2024-01-01 10:00:00'),
  (3, 'existing3', '2024-01-03 10:00:00')
""")

spark.sql("""
CREATE TABLE source (
  key INT,
  value STRING,
  created_at TIMESTAMP
) USING DELTA
""")

spark.sql("""
INSERT INTO source VALUES
  (1, 'source1', '2024-01-06 10:00:00'),  -- exists in target
  (2, 'source2', '2024-01-06 10:00:00'),  -- new
  (3, 'source3', '2024-01-06 10:00:00'),  -- exists in target
  (4, 'source4', '2024-01-06 10:00:00'),  -- new
  (5, 'source5', '2024-01-06 10:00:00')   -- new
""")

print("Tables created for INSERT unmatched rows example")

# COMMAND ----------

# Show before state
print("=== DATA BEFORE INSERT UNMATCHED ===")
print("\nTARGET TABLE:")
spark.sql("SELECT * FROM target ORDER BY key").show()

print("SOURCE TABLE:")
spark.sql("SELECT * FROM source ORDER BY key").show()

# COMMAND ----------

# Insert all rows from source that are not in target
spark.sql("""
MERGE INTO target USING source
ON target.key = source.key
WHEN NOT MATCHED THEN INSERT *
""")

print("INSERT unmatched rows completed")

# COMMAND ----------

# Show results
print("=== DATA AFTER INSERT UNMATCHED ===")
spark.sql("SELECT * FROM target ORDER BY key").show()

print(" Analysis:")
print("   - Keys 2, 4, 5: INSERTED (not in original target)")
print("   - Keys 1, 3: Unchanged (already existed in target)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2.2: WHEN NOT MATCHED - Conditional INSERT with DEFAULT Values

# COMMAND ----------

# Reset for conditional insert example
spark.sql("DROP TABLE IF EXISTS target")
spark.sql("DROP TABLE IF EXISTS source")

spark.sql("""
CREATE TABLE target (
  key INT,
  created_at TIMESTAMP,
  value STRING DEFAULT 'default_value',
  status STRING DEFAULT 'ACTIVE'
) USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

spark.sql("""
INSERT INTO target VALUES
  (1, '2024-01-01 10:00:00', 'existing1', 'ACTIVE')
""")

spark.sql("""
CREATE TABLE source (
  key INT,
  created_at TIMESTAMP,
  value STRING,
  status STRING
) USING DELTA
""")

# Insert mix of recent and old records
spark.sql(f"""
INSERT INTO source VALUES
  (1, '{time.strftime("%Y-%m-%d %H:%M:%S")}', 'source1', 'ACTIVE'),           -- exists
  (2, '{time.strftime("%Y-%m-%d %H:%M:%S")}', 'source2', 'PENDING'),         -- new, recent
  (3, '2024-01-01 10:00:00', 'source3', 'INACTIVE'),                         -- new, old
  (4, '{time.strftime("%Y-%m-%d %H:%M:%S")}', 'source4', 'PENDING')          -- new, recent
""")

print("Tables created for conditional INSERT with DEFAULT values example")

# COMMAND ----------

# Show before state
print("=== DATA BEFORE CONDITIONAL INSERT ===")
print("\nTARGET TABLE:")
spark.sql("SELECT * FROM target ORDER BY key").show()

print("SOURCE TABLE:")
spark.sql("SELECT * FROM source ORDER BY key").show()

# COMMAND ----------

# Conditionally insert new rows created in the last day, using DEFAULT for value
spark.sql(f"""
MERGE INTO target USING source
ON target.key = source.key
WHEN NOT MATCHED BY TARGET AND source.created_at > current_timestamp() - INTERVAL "1" DAY 
THEN INSERT (key, created_at, value) VALUES (source.key, source.created_at, DEFAULT)
""")

print("Conditional INSERT with DEFAULT values completed")

# COMMAND ----------

# Show results
print("=== DATA AFTER CONDITIONAL INSERT ===")
spark.sql("SELECT * FROM target ORDER BY key").show()

print(" Analysis:")
print("   - Keys 2, 4: INSERTED with DEFAULT value (recent timestamps)")
print("   - Key 3: NOT inserted (old timestamp, outside 1-day window)")
print("   - Key 1: Unchanged (already existed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: WHEN NOT MATCHED BY SOURCE Scenarios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3.1: WHEN NOT MATCHED BY SOURCE - DELETE Unmatched Target Rows

# COMMAND ----------

# Reset for NOT MATCHED BY SOURCE examples
spark.sql("DROP TABLE IF EXISTS target")
spark.sql("DROP TABLE IF EXISTS source")

spark.sql("""
CREATE TABLE target (
  key INT,
  value STRING,
  marked_for_deletion BOOLEAN
) USING DELTA
""")

spark.sql("""
INSERT INTO target VALUES
  (1, 'target1', false),
  (2, 'target2', false),
  (3, 'target3', true),
  (4, 'target4', false),
  (5, 'target5', false)
""")

spark.sql("""
CREATE TABLE source (
  key INT,
  value STRING
) USING DELTA
""")

# Source only has keys 1, 3, 5
spark.sql("""
INSERT INTO source VALUES
  (1, 'source1'),
  (3, 'source3'),
  (5, 'source5')
""")

print("Tables created for DELETE unmatched target rows example")

# COMMAND ----------

# Show before state
print("=== DATA BEFORE DELETE UNMATCHED BY SOURCE ===")
print("\nTARGET TABLE:")
spark.sql("SELECT * FROM target ORDER BY key").show()

print("SOURCE TABLE:")
spark.sql("SELECT key, value, NULL as marked_for_deletion FROM source ORDER BY key").show()

# COMMAND ----------

# Delete all target rows that have no matches in source
spark.sql("""
MERGE INTO target USING source
ON target.key = source.key
WHEN NOT MATCHED BY SOURCE THEN DELETE
""")

print("DELETE unmatched by source completed")

# COMMAND ----------

# Show results
print("=== DATA AFTER DELETE UNMATCHED BY SOURCE ===")
spark.sql("SELECT * FROM target ORDER BY key").show()

print(" Analysis:")
print("   - Keys 2, 4: DELETED (not found in source)")
print("   - Keys 1, 3, 5: Preserved (found in source)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3.2: WHEN NOT MATCHED BY SOURCE - Multiple Clauses

# COMMAND ----------

# Reset data for multiple NOT MATCHED BY SOURCE clauses
spark.sql("DROP TABLE target")
spark.sql("""
CREATE TABLE target (
  key INT,
  value STRING,
  marked_for_deletion BOOLEAN
) USING DELTA
""")

spark.sql("""
INSERT INTO target VALUES
  (1, 'target1', false),  -- in source
  (2, 'target2', true),   -- marked for deletion, not in source
  (3, 'target3', false),  -- in source
  (4, 'target4', false),  -- not in source
  (5, 'target5', false),  -- in source
  (6, 'target6', false)   -- not in source
""")

print("Target table reset for multiple NOT MATCHED BY SOURCE clauses")

# COMMAND ----------

# Show before state
print("=== DATA BEFORE MULTIPLE NOT MATCHED BY SOURCE ===")
print("\nTARGET TABLE:")
spark.sql("SELECT * FROM target ORDER BY key").show()

print("SOURCE TABLE:")
spark.sql("SELECT key, value, NULL as marked_for_deletion FROM source ORDER BY key").show()

# COMMAND ----------

# Multiple NOT MATCHED BY SOURCE clauses
spark.sql("""
MERGE INTO target USING source
ON target.key = source.key
WHEN NOT MATCHED BY SOURCE AND target.marked_for_deletion = true THEN DELETE
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET target.value = 'ORPHANED_' || target.value
""")

print("Multiple NOT MATCHED BY SOURCE clauses completed")

# COMMAND ----------

# Show results
print("=== DATA AFTER MULTIPLE NOT MATCHED BY SOURCE ===")
spark.sql("SELECT * FROM target ORDER BY key").show()

print(" Analysis:")
print("   - Key 2: DELETED (not in source AND marked_for_deletion = true)")
print("   - Keys 4, 6: Updated to 'ORPHANED_' (not in source but not marked for deletion)")
print("   - Keys 1, 3, 5: Unchanged (found in source)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: WITH SCHEMA EVOLUTION

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 4.1: Schema Evolution - Adding New Columns Automatically

# COMMAND ----------

# Reset for schema evolution
spark.sql("DROP TABLE IF EXISTS target")
spark.sql("DROP TABLE IF EXISTS source")

# Target has fewer columns initially
spark.sql("""
CREATE TABLE target (
  key INT,
  value STRING,
  updated_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
)
""")

spark.sql("""
INSERT INTO target VALUES
  (1, 'target1', '2024-01-01 10:00:00'),
  (2, 'target2', '2024-01-02 10:00:00'),
  (4, 'target4', '2024-01-04 10:00:00')
""")

# Source has additional columns
spark.sql("""
CREATE TABLE source (
  key INT,
  value STRING,
  updated_at TIMESTAMP,
  new_column STRING,
  another_new_column INT,
  metadata_json STRING
) USING DELTA
""")

spark.sql("""
INSERT INTO source VALUES
  (1, 'updated1', '2024-01-06 10:00:00', 'new_data1', 100, '{"type": "update"}'),  -- update existing
  (2, 'updated2', '2024-01-06 10:00:00', 'new_data2', 200, '{"type": "update"}'),  -- update existing  
  (3, 'new3', '2024-01-06 10:00:00', 'new_data3', 300, '{"type": "insert"}'),      -- insert new
  (5, 'new5', '2024-01-06 10:00:00', 'new_data5', 500, '{"type": "insert"}')       -- insert new
""")

print("Tables created for schema evolution example")

# COMMAND ----------

# Show schema differences before merge
print("=== SCHEMA COMPARISON BEFORE MERGE ===")
print("\nTARGET SCHEMA:")
spark.sql("DESCRIBE target").show()

print("SOURCE SCHEMA:")
spark.sql("DESCRIBE source").show()

# COMMAND ----------

# Show data before merge
print("=== DATA BEFORE SCHEMA EVOLUTION MERGE ===")
print("\nTARGET TABLE:")
spark.sql("SELECT * FROM target ORDER BY key").show()

print("SOURCE TABLE:")
spark.sql("SELECT * FROM source ORDER BY key").show()

# COMMAND ----------

# Multiple MATCHED and NOT MATCHED clauses with schema evolution
start_time = time.time()

spark.sql("""
MERGE WITH SCHEMA EVOLUTION INTO target USING source
ON source.key = target.key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
""")

end_time = time.time()
print(f"Schema evolution MERGE completed in {end_time - start_time:.3f} seconds")

# COMMAND ----------

# Show final results
print("=== DATA AFTER SCHEMA EVOLUTION MERGE ===")
spark.sql("SELECT * FROM target ORDER BY key").show()

# COMMAND ----------

# Verify the schema has evolved
print("=== TARGET SCHEMA AFTER EVOLUTION ===")
spark.sql("DESCRIBE target").show()

print("Schema Evolution Analysis:")
print("  - new_column: Added automatically")
print("  - another_new_column: Added automatically") 
print("  - metadata_json: Added automatically")
print("  - Existing data preserved with NULL values for new columns")

# COMMAND ----------

# Cleanup
spark.sql("DROP DATABASE IF EXISTS merge_demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Summary
# MAGIC
# MAGIC - Delta Lake's MERGE INTO enables robust CDC, upserts, and flexible evolution.
# MAGIC - All code in this notebook works with Unity Catalog, schema `{DEMO_SCHEMA}`, and catalog picked up from config.
# MAGIC - Both PySpark and SQL can be used for efficient, transactional data warehousing.
# MAGIC
# MAGIC Next: **[History Vaccuum TimeTravel →]($./08 - History_Vacuum_TimeTravel)**
# MAGIC