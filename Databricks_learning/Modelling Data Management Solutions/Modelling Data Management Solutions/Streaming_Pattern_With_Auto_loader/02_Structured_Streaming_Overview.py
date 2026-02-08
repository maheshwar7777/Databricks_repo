# Databricks notebook source
# MAGIC %md
# MAGIC ##2. Structured Streaming Overview
# MAGIC ###  What is Structured Streaming?
# MAGIC
# MAGIC Structured Streaming is a **scalable and fault-tolerant streaming engine** built on Spark SQL.  
# MAGIC It allows developers to express streaming computations in the same way as batch queries using DataFrame and SQL APIs.
# MAGIC
# MAGIC #### Responsibilities:
# MAGIC - Continuously process data as it arrives in near real-time
# MAGIC - Provide **exactly-once processing guarantees**
# MAGIC - Support schema inference and evolution during ingestion
# MAGIC - Enable checkpointing and recovery for fault tolerance
# MAGIC - Serve as the foundation for building reliable streaming pipelines
# MAGIC
# MAGIC #### Common Input Sources:
# MAGIC - Files arriving incrementally in cloud storage (e.g., AWS S3, Azure Data Lake, GCS)
# MAGIC - Message buses such as **Kafka** or **Kinesis**
# MAGIC - Event hubs or socket streams
# MAGIC
# MAGIC #### Common File Formats:
# MAGIC - JSON  
# MAGIC - CSV  
# MAGIC - Parquet  
# MAGIC - Avro  
# MAGIC - ORC  
# MAGIC