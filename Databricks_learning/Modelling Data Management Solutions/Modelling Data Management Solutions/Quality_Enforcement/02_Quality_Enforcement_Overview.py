# Databricks notebook source
# MAGIC %md
# MAGIC ## **2. Data Quality Overview**
# MAGIC
# MAGIC ### Responsibilities
# MAGIC - Validate incoming data to ensure accuracy and completeness  
# MAGIC - Monitor quality across pipelines with rules and metrics  
# MAGIC - Remediate by quarantining or correcting invalid records  
# MAGIC
# MAGIC ### Where to Apply
# MAGIC - **Ingest (Bronze):** catch schema drift, malformed records, unexpected fields  
# MAGIC - **Transform (Silver):** enforce business logic, deduplicate, standardize formats  
# MAGIC - **Publish (Gold):** validate KPIs and aggregates for decision-making  
# MAGIC
# MAGIC ### Key Building Blocks in Databricks
# MAGIC - **Constraints:** enforce NOT NULL, CHECK, and schema-level rules in Delta  
# MAGIC - **Expectations:** define pass/fail checks for columns and values  
# MAGIC - **Quarantine tables:** isolate bad data for review and reprocessing  
# MAGIC - **Rescued data:** automatically capture unexpected or malformed fields from Auto Loader  
# MAGIC