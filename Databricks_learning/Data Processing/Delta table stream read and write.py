# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Delta Lake Streaming with Stream-Static Joins
# MAGIC
# MAGIC This notebook demonstrates how to use **Delta Lake** with **Structured Streaming** in PySpark, including:
# MAGIC
# MAGIC - Streaming writes to a Delta table
# MAGIC - Static reads from a Delta table
# MAGIC - Stream-static joins with watermarking and time constraints
# MAGIC - Stream changes 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate Streaming Input

# COMMAND ----------

# MAGIC %md
# MAGIC This code simulates a streaming data source in PySpark using the rate format, which generates rows at a fixed rate (5 rows/sec). It then transforms the stream by renaming the default columns to eventId and eventTime, mimicking a real-time event stream.

# COMMAND ----------


from pyspark.sql.functions import expr

# Simulate a stream using rate source
input_stream = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

# Transform the stream
events_stream = input_stream.selectExpr("value as eventId", "timestamp as eventTime")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Streaming Data to Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC This code writes the simulated streaming data (events_stream) to a Delta Lake table in append mode, ensuring fault tolerance using a checkpoint directory. It's a common pattern for building reliable, scalable streaming pipelines with Apache Spark and Delta Lake.

# COMMAND ----------


# Write the stream to a Delta table

query = events_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "dbfs:/user/checkpoints/events") \
    .toTable("data_streaming.streamjoin.event_stream_data")
query.awaitTermination()



# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Static Data from Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC This code reads the stored Delta table as a static DataFrame, allowing you to analyze or visualize the data that was previously written by the streaming job.

# COMMAND ----------


# Read the Delta table as a static DataFrame
static_events_df = spark.read.table("data_streaming.streamjoin.event_stream_data")
display(static_events_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream-Static Join with Watermarking and Time Constraints

# COMMAND ----------

# MAGIC %md
# MAGIC This code performs a stream-static join in PySpark, where a simulated event stream is joined with static reference data using event ID and time constraints. Watermarking ensures late data is handled gracefully, and the result is streamed to the console in real time.
# MAGIC

# COMMAND ----------


from pyspark.sql.functions import expr

#Alias both DataFrames
events_stream_with_watermark = events_stream.withWatermark("eventTime", "1 minute").alias("stream")
static_events_df = static_events_df.alias("static")
static_events_df_renamed = static_events_df.withColumnRenamed("eventId", "static_eventId").withColumnRenamed("eventTime", "static_eventTime")

# Join using fully qualified column names
joined_df = events_stream_with_watermark.join(
    static_events_df_renamed,
    expr("""
        stream.eventId = static_eventId AND
        stream.eventTime >= static_eventTime AND
        stream.eventTime <= static_eventTime + interval 1 minute
    """)
)
joined_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "dbfs:/user/checkpoints/events") \
    .toTable("data_streaming.streamjoin.stream_static_joined")
# joined_events_df = spark.read.table("data_streaming.default.test_data_joined")
# display(joined_events_df)



# COMMAND ----------

# MAGIC %md
# MAGIC # Stream incremental changes from a Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC When it comes to streaming changes from a Delta table for incremental processing, there are two options to consider:
# MAGIC
# MAGIC 1.Stream from a change data capture (CDC) feed of a Delta table (more robhust).
# MAGIC
# MAGIC 2.Stream from the Delta table itself.

# COMMAND ----------

# MAGIC %md
# MAGIC # Option 1: Stream from a change data capture (CDC) feed

# COMMAND ----------

# MAGIC %md
# MAGIC - CDF allows you to track row-level changes (inserts, updates, deletes) between versions of a Delta table.
# MAGIC - When enabled, CDF records change events for all data written into the table, including metadata indicating the type of change.
# MAGIC - CDF works with table history, but cloned tables have separate histories, so their CDFs do not match.

# COMMAND ----------

# MAGIC %md
# MAGIC **How to Enable Change Data Feed**

# COMMAND ----------

# MAGIC %sql
# MAGIC --new table
# MAGIC CREATE TABLE student (id INT, name STRING, age INT)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --existing table
# MAGIC ALTER TABLE myDeltaTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Globally
# MAGIC SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Change Data Feed**

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Streaming Reads**
# MAGIC
# MAGIC Use Structured Streaming with the readChangeFeed option:
# MAGIC

# COMMAND ----------


spark.readStream
  .option("readChangeFeed", "true")
  .table("data_streaming.streamjoin.event_stream_data")


# COMMAND ----------

# MAGIC %md
# MAGIC - By default, the stream returns the latest snapshot as INSERTs, and future changes as change data.
# MAGIC - You can specify a starting version to ignore earlier changes:
# MAGIC

# COMMAND ----------


spark.readStream\
  .option("readChangeFeed", "true")\
  .option("startingVersion", 76)\
  .table("data_streaming.streamjoin.event_stream_data")


# COMMAND ----------

# MAGIC %md
# MAGIC **2. Batch Queries**
# MAGIC
# MAGIC Read all changes from a specific version or within a range:
# MAGIC

# COMMAND ----------

spark.read \
  .option("readChangeFeed", "true") \
  .option("startingTimestamp", '2021-04-21 05:45:46') \
  .option("endingTimestamp", '2021-05-21 12:00:00') \
  .table("data_streaming.streamjoin.event_stream_data")


# COMMAND ----------

# MAGIC %md
# MAGIC The schema includes metadata columns:
# MAGIC
# MAGIC - _change_type: insert, update_preimage, update_postimage, delete
# MAGIC - _commit_version: Delta log/table version
# MAGIC - _commit_timestamp: Commit timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC # Option 2: Stream from a Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC Structured Streaming incrementally reads Delta tables. While a streaming query is active against a Delta table, new records are processed idempotently as new table versions commit to the source table.

# COMMAND ----------

# read stream from a table
spark.readStream.table("data_streaming.streamjoin.event_stream_data")
# read stream from a path
spark.readStream.load("/path/to/table")

# COMMAND ----------

# MAGIC %md
# MAGIC **Handling Schema Changes**
# MAGIC
# MAGIC - If the schema of the Delta table changes after the stream starts, the query fails.
# MAGIC - You can restart the stream to resolve schema mismatches.
# MAGIC - Column mapping + non-additive schema evolution (e.g., renaming or dropping columns) is not supported in Databricks Runtime ≤ 12.2 LTS.

# COMMAND ----------

# MAGIC %md
# MAGIC **Limit input rate**
# MAGIC
# MAGIC **maxFilesPerTrigger:** How many new files to be considered in every micro-batch. The default is 1000.
# MAGIC
# MAGIC **maxBytesPerTrigger:** How much data gets processed in each micro-batch.

# COMMAND ----------

spark.readStream\
.option("maxFilesPerTrigger", 1000) \
.option("maxBytesPerTrigger", 10485760)  \
.table("data_streaming.streamjoin.event_stream_data")

# COMMAND ----------

# MAGIC %md
# MAGIC **Handling Data Loss**
# MAGIC
# MAGIC If older versions are cleaned up due to logRetentionDuration, the stream may fail.

# COMMAND ----------

spark.readStream\
.option("failOnDataLoss", "false")\
.table("data_streaming.streamjoin.event_stream_data")

# COMMAND ----------

# MAGIC %md
# MAGIC **Ignoring Updates and Deletes**
# MAGIC
# MAGIC Structured Streaming expects append-only data. To handle changes:

# COMMAND ----------

#skipChangeCommits - ignores all file-changing operations (UPDATE, DELETE, MERGE).
spark.readStream.option("skipChangeCommits", "true").table("data_streaming.streamjoin.event_stream_data")

#ignoreDeletes - only works for full partition drops.
spark.readStream.option("ignoreDeletes", "true").table("data_streaming.streamjoin.event_stream_data")


# COMMAND ----------

# MAGIC %md
# MAGIC **Specify Initial Position**
# MAGIC
# MAGIC

# COMMAND ----------

# By Version
spark.readStream \
  .option("startingVersion", "5") \
  .table("data_streaming.streamjoin.event_stream_data")

# By Timestamp
spark.readStream \
  .option("startingTimestamp", "2018-10-18") \
  .table("data_streaming.streamjoin.event_stream_data ")

# COMMAND ----------

# MAGIC %md
# MAGIC **Initial Snapshot Processing**
# MAGIC
# MAGIC - When a stream starts, it processes the initial snapshot of the table.
# MAGIC - This includes all data present at the starting version or timestamp.

# COMMAND ----------

# MAGIC %md
# MAGIC # Output modes

# COMMAND ----------

# append mode - adds new records to the table

events_stream.writeStream\
.outputMode("append")\
.option("checkpointLocation", "/tmp/delta/events/_checkpoints/")\
.toTable("events")

# complete mode - replace the entire table with every batch

spark.readStream\
  .table("events")\
  .groupBy("eventId")\
  .count()\
  .writeStream\
  .outputMode("complete")\
  .option("checkpointLocation", "/tmp/delta/eventsByCustomer/_checkpoints/")\
  .toTable("events_by_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC # Upsert from streaming queries using foreachBatch

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC writeStream with .toTable() or .save() only supports append or overwrite modes — not merge/upsert.
# MAGIC
# MAGIC So, if you want to merge streaming data into an existing Delta table, you use foreachBatch.
# MAGIC
# MAGIC You can use a combination of merge and foreachBatch to write complex upserts from a streaming query into a Delta 
# MAGIC table.
# MAGIC
# MAGIC **Use Cases**
# MAGIC
# MAGIC
# MAGIC **Write streaming aggregates in Update Mode**
# MAGIC - More efficient than Complete Mode.
# MAGIC - Suitable for streaming aggregations where only updated results need to be written.
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Write a stream of database changes into a Delta table**
# MAGIC
# MAGIC - Use merge inside foreachBatch to apply change data capture (CDC) logic.
# MAGIC - Continuously apply inserts, updates, and deletes from a stream to a Delta table.
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Write a stream of data into a Delta table with deduplication**
# MAGIC
# MAGIC - Use an insert-only merge strategy.
# MAGIC - Deduplicate records (e.g., based on a unique ID or timestamp) before writing to avoid duplicates.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDelta(microBatchOutputDF, batchId):
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("updates")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe

  # In Databricks Runtime 10.5 and below, you must use the following:
  # microBatchOutputDF._jdf.sparkSession().sql("""
  microBatchOutputDF.sparkSession.sql("""
    MERGE INTO data_streaming.streamjoin.event_stream_data t
    USING data_streaming.default.stream_static_joined s
    ON s.eventId = t.eventId
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

# Write the output of a streaming aggregation query into Delta table
events_stream.writeStream\
  .foreachBatch(upsertToDelta)\
  .outputMode("update")\
  .start()


# COMMAND ----------

# MAGIC %md
# MAGIC # Idempotent table writes in foreachBatch

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Delta tables support the following **DataFrameWriter** options to make writes to multiple tables within foreachBatch idempotent:
# MAGIC
# MAGIC txnAppId: A unique string that you can pass on each DataFrame write. For example, you can use the StreamingQuery ID as txnAppId.
# MAGIC
# MAGIC txnVersion: A monotonically increasing number that acts as transaction version.
# MAGIC
# MAGIC It ensures streaming data is written to Delta tables exactly once, even in the face of failures or retries, ensuring high data quality and reliability.

# COMMAND ----------

app_id = "test_app" # A unique string that is used as an application ID.

def writeToDeltaLakeTableIdempotent(batch_df, batch_id):
  
  batch_df.write.format("delta").option("txnVersion", batch_id).option("txnAppId", app_id).mode("append").saveAsTable("data_streaming.streamjoin.idempotent_location1") # location 1
  batch_df.write.format("delta").option("txnVersion", batch_id).option("txnAppId", app_id).mode("append").saveAsTable("data_streaming.streamjoin.idenpotent_location2") # location 2

events_stream.writeStream.foreachBatch(writeToDeltaLakeTableIdempotent).start()