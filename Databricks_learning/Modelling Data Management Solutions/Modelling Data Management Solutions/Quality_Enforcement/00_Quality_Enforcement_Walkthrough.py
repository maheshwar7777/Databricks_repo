# Databricks notebook source
# MAGIC %md
# MAGIC ## **1. Introduction**
# MAGIC
# MAGIC ### Data Quality (DQ)
# MAGIC
# MAGIC Data Quality (DQ) in Databricks ensures that data ingested and transformed across the **Medallion architecture** is accurate, consistent, and trustworthy.  
# MAGIC It provides the foundation for reliable analytics and machine learning by applying validations and rules to detect, quarantine, and remediate bad data.
# MAGIC
# MAGIC - **What is Data Quality (DQ)?**  
# MAGIC   A framework of checks and constraints (such as null handling, type validation, and business rules) that verify the correctness of data as it flows through pipelines.
# MAGIC
# MAGIC - **Role in the Medallion Architecture (Bronze → Silver → Gold)**  
# MAGIC   This notebook demonstrates how to apply **data quality rules** at different stages:  
# MAGIC   - **Bronze**: capture schema drift, track anomalies, store rescued data.  
# MAGIC   - **Silver**: enforce constraints, deduplicate, and separate invalid records into quarantine tables.  
# MAGIC   - **Gold**: validate KPIs and aggregates to ensure trusted business outcomes.  
# MAGIC
# MAGIC By integrating DQ into each layer, Databricks helps teams maintain governance, compliance, and confidence in their data products.
# MAGIC