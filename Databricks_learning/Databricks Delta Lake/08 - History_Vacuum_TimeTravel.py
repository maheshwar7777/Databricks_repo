# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4: Delta Lake History, Vacuum, and Time Travel
# MAGIC
# MAGIC This module covers Delta Lake's capabilities for viewing table history, cleaning up old data with VACUUM, and querying previous versions with Time Travel.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Understand table operation history and how to retrieve it
# MAGIC - Safely reclaim storage space using VACUUM (including LITE mode)
# MAGIC - Use Delta Lake Time Travel for audits, restores, and reproducible analytics
# MAGIC
# MAGIC ## Reference Documentation
# MAGIC **Databricks Official Documentation:** 
# MAGIC [Vacuum](https://docs.databricks.com/en/delta/vacuum.html), [Time Travel](https://docs.databricks.com/en/delta/history.html)
# MAGIC

# COMMAND ----------

from Includes.config import *

from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Set catalog and schema context
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {DEMO_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Table History (DESCRIBE HISTORY)
# MAGIC
# MAGIC Each write operation on a Delta table is versioned and logged. You can retrieve the complete history and important metadata for audit, troubleshooting, and lineage purposes.
# MAGIC
# MAGIC |Column|Description|
# MAGIC |--|--|
# MAGIC |version|Table version number|
# MAGIC |timestamp|Commit time|
# MAGIC |userId|ID of user|
# MAGIC |operation|Operation type (WRITE, UPDATE, DELETE, MERGE, etc)|
# MAGIC |operationMetrics|Metrics (rows/bytes/files added, updated, etc)|

# COMMAND ----------

# Create a small example table for history demo
from pyspark.sql import Row

demo_data = [Row(id=1, name="Alice"), Row(id=2, name="Bob")]
df = spark.createDataFrame(demo_data)
table_name = "history_demo"

df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{DEMO_SCHEMA}.{table_name}")

# Do a quick update and delete to generate some history
spark.sql(f"UPDATE {CATALOG}.{DEMO_SCHEMA}.{table_name} SET name = 'Alice Wonderland' WHERE id = 1")
spark.sql(f"DELETE FROM {CATALOG}.{DEMO_SCHEMA}.{table_name} WHERE id = 2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Full Table History (PySpark)
# MAGIC
# MAGIC This provides audit information about all changes to the table.

# COMMAND ----------

history_df = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{DEMO_SCHEMA}.{table_name}")
display(history_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Last Operation Only (SQL)
# MAGIC
# MAGIC ```
# MAGIC DESCRIBE HISTORY history_demo LIMIT 1
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY history_demo LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Vacuum (Data File Clean-Up)
# MAGIC
# MAGIC **VACUUM** removes data files no longer referenced by a Delta table that are older than the retention threshold, reclaiming cloud storage and ensuring records deleted/modified are no longer accessible.
# MAGIC
# MAGIC - The **default retention period is 7 days**.
# MAGIC - VACUUM only removes data files, never transaction log files.
# MAGIC - Running VACUUM too frequently with a low retention period risks losing undeleted versions.
# MAGIC - **VACUUM LITE** mode (Databricks Runtime 16.1+) provides an optimized approach for large tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dry-Run (Preview What Would Be Deleted)
# MAGIC

# COMMAND ----------

spark.sql(f"VACUUM {CATALOG}.{DEMO_SCHEMA}.{table_name} DRY RUN").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Standard VACUUM (PySpark)
# MAGIC
# MAGIC This will clean up files not required by Delta after 7 days (by default).

# COMMAND ----------

spark.sql(f"VACUUM {CATALOG}.{DEMO_SCHEMA}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run VACUUM with Custom Retention (48 hours)
# MAGIC

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM {CATALOG}.{DEMO_SCHEMA}.{table_name} RETAIN 48 HOURS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2.1: VACUUM LITE Mode (Databricks Runtime 16.1+)
# MAGIC
# MAGIC **VACUUM LITE** is an optimized vacuum mode that avoids listing all files in the table directory by using the Delta transaction log to identify data files to remove. This is especially useful for:
# MAGIC - Large tables that require frequent VACUUM operations
# MAGIC - Tables with many files where file listing is slow
# MAGIC - Reducing the overhead of vacuum operations
# MAGIC
# MAGIC ### Requirements for VACUUM LITE:
# MAGIC - Must have run at least one successful VACUUM operation within the transaction log retention threshold (30 days by default)
# MAGIC - Only removes files referenced in the transaction log (won't clean up files from aborted transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run VACUUM LITE (SQL)
# MAGIC
# MAGIC LITE mode uses the Delta transaction log to identify files to remove, avoiding the need to list all files in the table directory.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run VACUUM in LITE mode (requires at least one previous successful VACUUM)
# MAGIC VACUUM history_demo LITE;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run VACUUM LITE with Custom Retention (PySpark)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# Run VACUUM LITE with custom retention period
spark.sql(f"VACUUM {CATALOG}.{DEMO_SCHEMA}.{table_name} LITE RETAIN 24 HOURS")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run VACUUM FULL (Explicit Full Mode)
# MAGIC
# MAGIC You can explicitly specify FULL mode, which is the default behavior that lists all files in the table directory.

# COMMAND ----------

spark.sql(f"VACUUM {CATALOG}.{DEMO_SCHEMA}.{table_name} FULL")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explicit FULL mode vacuum
# MAGIC VACUUM history_demo FULL RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM LITE Error Handling
# MAGIC
# MAGIC If you haven't run a successful VACUUM within the transaction log retention period, VACUUM LITE will fail with an error message:
# MAGIC ```
# MAGIC VACUUM <tableName> LITE cannot delete all eligible files as some files are not referenced by the Delta log. Please run VACUUM FULL.
# MAGIC ```
# MAGIC
# MAGIC In this case, you must run VACUUM in FULL mode first.

# COMMAND ----------

# Example of handling VACUUM LITE error scenario
try:
    spark.sql(f"VACUUM {CATALOG}.{DEMO_SCHEMA}.{table_name} LITE")
    print("VACUUM LITE completed successfully")
except Exception as e:
    if "cannot delete all eligible files" in str(e):
        print("VACUUM LITE failed - running VACUUM FULL instead")
        spark.sql(f"VACUUM {CATALOG}.{DEMO_SCHEMA}.{table_name} FULL")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Equivalents Summary:
# MAGIC ```
# MAGIC -- Standard vacuum operations
# MAGIC VACUUM history_demo;
# MAGIC VACUUM history_demo RETAIN 24 HOURS;
# MAGIC VACUUM history_demo DRY RUN;
# MAGIC
# MAGIC -- LITE mode vacuum (Runtime 16.1+)
# MAGIC VACUUM history_demo LITE;
# MAGIC VACUUM history_demo LITE RETAIN 48 HOURS;
# MAGIC
# MAGIC -- Explicit FULL mode vacuum
# MAGIC VACUUM history_demo FULL;
# MAGIC VACUUM history_demo FULL RETAIN 72 HOURS;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Delta Lake Time Travel
# MAGIC
# MAGIC You can query, read, or restore from previous table versions by either UNDOING accidental operations or auditing records from the past.  
# MAGIC Time travel works by specifying a `VERSION` or a `TIMESTAMP` (assuming retention period is sufficient).
# MAGIC
# MAGIC **Common use cases:**
# MAGIC - Reproduce an analysis with old data (debug/audit)
# MAGIC - Undo deletes or updates
# MAGIC - Compare/restore historical states (including for ML model reproducibility)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Previous Version (SQL)
# MAGIC ```
# MAGIC SELECT * FROM history_demo VERSION AS OF 0;
# MAGIC SELECT * FROM history_demo TIMESTAMP AS OF '2024-09-03 09:15:00';
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM history_demo VERSION AS OF 0;
# MAGIC -- SELECT * FROM history_demo TIMESTAMP AS OF '2024-09-03 09:15:00';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Previous Version (PySpark)
# MAGIC
# MAGIC Example: get all data as it existed in version 0 (table creation).

# COMMAND ----------

df_old = spark.read.option("versionAsOf", 0).table(f"{CATALOG}.{DEMO_SCHEMA}.{table_name}")
df_old.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restore Table to Previous State
# MAGIC Easily undo a bad delete/update or bring back the table to an earlier version.

# COMMAND ----------

spark.sql(f"RESTORE TABLE {CATALOG}.{DEMO_SCHEMA}.{table_name} TO VERSION AS OF 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE history_demo TO VERSION AS OF 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Best Practices and Caveats
# MAGIC
# MAGIC ### General Best Practices:
# MAGIC - Delta table history (`DESCRIBE HISTORY`) is kept for 30 days by default (`delta.logRetentionDuration`)
# MAGIC - Data files are only available for as long as no vacuum runs with a lower retention period (`delta.deletedFileRetentionDuration`)
# MAGIC - **VACUUM is IRREVERSIBLE**. Permanent file deletion cannot be undone.
# MAGIC - Set retention intervals longer than your longest-running job—**never less than 7 days** unless you're sure no jobs need older versions.
# MAGIC - **Time travel loses data** if the files have been vacuumed (even if history log is present).
# MAGIC - For audit and debugging, always check which files/versions are safe before vacuum.
# MAGIC
# MAGIC ### VACUUM LITE Specific Best Practices:
# MAGIC - Use VACUUM LITE for large tables with frequent vacuum operations to improve performance
# MAGIC - Ensure you've run at least one successful VACUUM FULL before using LITE mode
# MAGIC - VACUUM LITE won't clean up files from aborted transactions - run VACUUM FULL periodically for complete cleanup
# MAGIC - Monitor transaction log retention settings when using LITE mode regularly
# MAGIC
# MAGIC ### Performance Considerations:
# MAGIC - **VACUUM LITE**: Faster for large tables as it avoids file listing operations
# MAGIC - **VACUUM FULL**: More thorough cleanup but slower for tables with many files
# MAGIC - Consider table size, file count, and vacuum frequency when choosing between LITE and FULL modes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **DESCRIBE HISTORY** provides table audit and operation history.
# MAGIC - **VACUUM** reclaims cloud storage, subject to retention:
# MAGIC   - **VACUUM FULL** (default): Lists all files, thorough cleanup
# MAGIC   - **VACUUM LITE** (Runtime 16.1+): Uses transaction log, optimized for large tables
# MAGIC - **Time travel** makes data rollback, restore, and auditing simple—if files/logs have not expired.
# MAGIC - Choose VACUUM mode based on table size, performance requirements, and cleanup frequency.
# MAGIC
# MAGIC Next: **[Delta Transaction Logs →]($./09 - Delta_Transaction_Logs)**