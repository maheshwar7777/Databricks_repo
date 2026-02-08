# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Deduplication overview
# MAGIC - Deduplication in Databricks can be efficiently handled using Delta Lake's MERGE operation. This operation allows you to merge data from a source table into a target Delta table, updating existing records and inserting new ones.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, "/path/to/delta-table")

deltaTable.alias("target").merge(
sourceDF.alias("source"),
"target.id = source.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC - In this above example, the MERGE operation updates rows in the target table where the id matches and inserts new rows where there is no match.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Deduplication
# MAGIC - For streaming data, you can use the dropDuplicates function with watermarking to handle deduplication. This ensures that only unique records within a specified time window are processed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example

# COMMAND ----------

stream_df = (
spark.readStream.format("delta")
.table("source_table")
.withWatermark("event_time", "5 minutes")
.dropDuplicates(["event_id"])
)

stream_df.writeStream.format("delta").option("checkpointLocation", "/path/to/checkpoint").table("output_table")

# COMMAND ----------

# MAGIC %md
# MAGIC - In this example, records are deduplicated based on the event_id within a 5-minute window
# MAGIC
# MAGIC ### Considerations
# MAGIC - **Global State:** Using dropDuplicates without specifying a time window can lead to an unbounded state, potentially causing out-of-memory issues
# MAGIC - **Watermarking:** Properly setting the watermark ensures that late-arriving records are handled correctly
# MAGIC - **Performance:** Partitioning your data by date or other relevant columns can improve performance and efficiency

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implementation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create Sample Streaming Data

# COMMAND ----------

# MAGIC %md
# MAGIC We’ll simulate streaming by writing data continuously into a Delta table (source).

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, expr
import time

# Create a static DataFrame with fake events
data = [
    (1, "click"),
    (2, "view"),
    (3, "purchase"),
    (1, "click"),      # duplicate
    (2, "view"),       # duplicate
    (4, "signup")
]

df = spark.createDataFrame(data, ["event_id", "event_type"]) \
          .withColumn("event_time", current_timestamp())

# Write initial data to a Delta source table
df.write.format("delta").mode("overwrite").saveAsTable("source_table")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Start a Continuous Stream

# COMMAND ----------

# MAGIC %md
# MAGIC Here we’ll read from the source_table and apply deduplication with watermarking.

# COMMAND ----------

# Read streaming data
stream_df = (
    spark.readStream
        .format("delta")
        .table("source_table")
        .withWatermark("event_time", "5 minutes")
        .dropDuplicates(["event_id"])
)

# Write deduplicated output to another Delta table
query = (
    stream_df.writeStream
        .format("delta")
        .option("checkpointLocation", "/tmp/checkpoints/stream_dedup_demo")
        .outputMode("append")
        .table("output_table")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Simulate Streaming Inserts

# COMMAND ----------

# MAGIC %md
# MAGIC append new data batches to the source table.

# COMMAND ----------

for i in range(3):
    new_data = [
        (i+5, "event_" + str(i)),
        (1, "click"),  # duplicate event_id
    ]
    new_df = spark.createDataFrame(new_data, ["event_id", "event_type"]) \
                  .withColumn("event_time", current_timestamp())
    
    new_df.write.format("delta").mode("append").saveAsTable("source_table")
    time.sleep(5)  # simulate delay


# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the Deduplicated Output

# COMMAND ----------

display(spark.table("output_table"))