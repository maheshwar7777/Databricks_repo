# Databricks notebook source
# MAGIC %md
# MAGIC #What is DLT META CLI?
# MAGIC **DLT META** is an open-source, metadata-driven framework (from Databricks Labs) that automates the creation and orchestration of **bronze and silver pipelines** using **Delta Live Tables (DLT)**. Instead of manually coding pipelines for each table, you define your sources, targets, data quality rules, and transformations through metadata (e.g., JSON/YAML). The framework then dynamically generates and executes the pipelines.
# MAGIC
# MAGIC
# MAGIC **DLT META CLI** (`databricks labs dlt-meta`) offers two key commands:
# MAGIC - onboard: to ingest metadata and prepare DataflowSpecs (dataflow definitions).
# MAGIC - deploy: to generate and launch the corresponding DLT pipelines (bronze and/or silver). 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #How to Implement DLT META CLI

# COMMAND ----------

# MAGIC %md
# MAGIC ####Prerequisites -
# MAGIC - Python 3.8+

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 : Install databricks CLI v0.213 or later
# MAGIC
# MAGIC **Command:**
# MAGIC `pip install databricks-cli` 
# MAGIC
# MAGIC Note : If above command not installed latest version, use below command
# MAGIC
# MAGIC `winget search databricks`
# MAGIC
# MAGIC `winget install Databricks.DatabricksCLI`
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 : Authenticate Databricks CLI with workspace
# MAGIC
# MAGIC ######Authenticate workspace using Databricks UI:
# MAGIC **Command:**
# MAGIC `databricks auth login --host <your-workspace-url>`
# MAGIC
# MAGIC Note : This sets up access to your workspace from your terminal.
# MAGIC
# MAGIC ######Authenticate workspace using PAT:
# MAGIC
# MAGIC **Command:**
# MAGIC `databricks configure -token`
# MAGIC
# MAGIC Provide below details:
# MAGIC
# MAGIC -	Databricks Host: Enter your  Databricks workspace URL (e.g. https://dbc-########-###1e.cloud.databricks.com)
# MAGIC
# MAGIC -	Token: Generate a token from the Databricks UI  
# MAGIC User Settings → Developer → Access Tokens
# MAGIC                (e.g, dapi864543##########63d0272####7d8c2-3)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 : Install dlt-meta library
# MAGIC
# MAGIC **Command:** 
# MAGIC `databricks labs install dlt-meta`
# MAGIC
# MAGIC Note : This installs the dlt-meta plugin under the new Databricks Labs CLI framework
# MAGIC
# MAGIC
# MAGIC **You can test the CLI is ready: `databricks labs dlt-meta --help`**
# MAGIC
# MAGIC You should see CLI options like `onboard`, `deploy`, etc.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 : Onboard Metadata
# MAGIC
# MAGIC **Command:** 
# MAGIC `databricks labs dlt-meta onboard`
# MAGIC
# MAGIC **This interactive step will prompt you for:**
# MAGIC - Paths to onboarding metadata (JSON/YAML)
# MAGIC - Target database or schema
# MAGIC - Whether to enable Unity Catalog
# MAGIC - Layer selection (bronze / silver / bronze_silver)
# MAGIC - Versioning, environment, and other preferences
# MAGIC
# MAGIC
# MAGIC Once complete, it stores the metadata as DataflowSpec tables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5 : Deploy DLT Pipelines
# MAGIC
# MAGIC **Command:** 
# MAGIC `databricks labs dlt-meta deploy`
# MAGIC
# MAGIC
# MAGIC **This prompts for details such as:**
# MAGIC - Unity Catalog settings
# MAGIC - Layer to deploy (bronze or silver or bronze_silver)
# MAGIC - Pipeline names, schemas, datasets, etc.
# MAGIC
# MAGIC It then generates and triggers the corresponding DLT pipelines in Databricks.
# MAGIC