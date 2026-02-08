# Databricks notebook source
# MAGIC %md
# MAGIC ##**1. Introduction**
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Structured Streaming
# MAGIC
# MAGIC Structured Streaming in Databricks is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine.  
# MAGIC It allows developers to express streaming computations the same way as batch queries using DataFrame and SQL APIs.
# MAGIC
# MAGIC - **What is Structured Streaming?**  
# MAGIC   A micro-batch and continuous processing engine that can process data in near real-time while providing exactly-once guarantees.
# MAGIC
# MAGIC - **Role in Streaming Tutorial**  
# MAGIC   This notebook (*00_Streaming_From_Multiplex_Bronze*) demonstrates how to use **Databricks Auto Loader** with Structured Streaming to continuously ingest JSON files from object storage into the **Bronze layer**.  
# MAGIC   It shows how schema inference, checkpointing, and incremental file discovery enable robust streaming ingestion patterns in Databricks.
# MAGIC
# MAGIC
# MAGIC