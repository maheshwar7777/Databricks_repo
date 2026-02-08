# Databricks notebook source
# MAGIC %md
# MAGIC #Getting Started with DLT-META
# MAGIC
# MAGIC **DLT-META** is a **metadata-driven framework** developed by **Databricks Labs** to automate the creation and management of **Delta Live Tables (DLT)** pipelines. It uses a JSON-based specification (Dataflowspec) to define source/target tables, transformations, CDC logic, and data quality rules, enabling scalable and reusable pipeline development.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Key Features of DLT-META
# MAGIC - **Metadata-Driven Automation**
# MAGIC   - Uses JSON/YAML files to define pipeline logic, source/target mappings, transformations, and data quality rules.
# MAGIC   - Eliminates the need for writing custom Python or SQL code for each pipeline.
# MAGIC
# MAGIC - **Bronze and Silver Pipeline Support**
# MAGIC   - Automates ingestion (bronze layer) and transformation (silver layer) pipelines.
# MAGIC   - Supports CDC (Change Data Capture) and append flows for dynamic data updates.
# MAGIC
# MAGIC - **Data Quality Enforcement**
# MAGIC   - Allows defining data quality expectations in metadata.
# MAGIC   - Invalid records can be routed to quarantine tables for review.
# MAGIC
# MAGIC - **Scalability & Reusability**
# MAGIC   - Centralized metadata enables consistent logic across hundreds of datasets.
# MAGIC   - Pipelines are modular and reusable, making onboarding new datasets fast and error-free.
# MAGIC
# MAGIC - **Advanced Features**
# MAGIC   - Supports event-driven processing, real-time data quality metrics, visual DAGs, and SCD Type 2 logic.
# MAGIC
# MAGIC - **Auditability & Lineage**
# MAGIC   -racks pipeline changes and data lineage for compliance and governance.
# MAGIC
# MAGIC
# MAGIC
# MAGIC More details: [Databricks Product Page](https://docs.databricks.com/aws/en/dlt-ref/dlt-meta)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Agenda
# MAGIC
# MAGIC 1. Review pipeline configurations [config.py]($./include/config.py)
# MAGIC 2. Generate sample data using  [data_generation]($./include/data_generation)
# MAGIC 3. Generate Onboarding json configurations (On boarding json, DQE, DDL and Transformation) [create_onboarding_json_file]($./include/create_onboarding_json_file)
# MAGIC 4. DLT-META onboarding metadata setup [01-DLT_META_onboarding_set_up]($./01-DLT_META_onboarding_step_up)
# MAGIC 5. Create and run a DLT-META pipeline 
# MAGIC 6. Explore DLT DAG and event logs
# MAGIC 7. Row and Column level security

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Install Faker and DLT-META libraries and review configuration values

# COMMAND ----------

# MAGIC %pip install iso3166 Faker
# MAGIC %pip install dlt-meta 
# MAGIC %restart_python
# MAGIC # %load_ext autoreload
# MAGIC # %autoreload 2

# COMMAND ----------

from include.config import *
from include.dlt_meta_utility import *
%load_ext autoreload
%autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.1 Display the config values

# COMMAND ----------

print_config_values()

# COMMAND ----------

# MAGIC %md
# MAGIC ###2&3. Call onboading json notebook
# MAGIC
# MAGIC **This notebook perform below operations:**
# MAGIC - Step up catalog, schema and volumn
# MAGIC - Generate sample data csv files
# MAGIC - Generate onboarding json , dqe and transformation file

# COMMAND ----------

# MAGIC %run ./include/create_onboarding_json_file

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. Run below notebook to onboard onboarding json configuration to dataflowspec tables

# COMMAND ----------

# MAGIC %run ./01-DLT_META_onboarding_step_up

# COMMAND ----------

# MAGIC %md
# MAGIC ###5. Create DLT-META pipeline
# MAGIC
# MAGIC #####Choose one of the two options to create your Delta Live Table (DLT) pipeline:
# MAGIC
# MAGIC **Option 1:** DLT_META CLI
# MAGIC
# MAGIC **Option 2:** DLT-META manual job
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Option 1:** DLT_META CLI
# MAGIC
# MAGIC [Steps for dlt-meta CLI implementation]($./03-DLT_META_CLI)
# MAGIC
# MAGIC Refer below databricks dtl-meta documentation for CLI :
# MAGIC
# MAGIC [DLT-META CLI implementation](https://databrickslabs.github.io/dlt-meta/getting_started/dltmeta_cli/#onboardjob)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Option 2:** DLT-META manual job
# MAGIC
# MAGIC To create your DLT-META pipeline in Lakeview Declarative Pipelines:
# MAGIC
# MAGIC 1. Go to **Jobs & Pipelines** > click **New > ETL Pipeline**.
# MAGIC 2. Enter a unique **Pipeline name**.
# MAGIC 3. Set **Product Edition** to **Advanced** (default).
# MAGIC 4. Set **Pipeline mode** to **Triggered** (uses AvailableNow trigger).
# MAGIC 5. Under **Notebook Libraries**, select the notebook named **[02-Generic_dlt_meta_data_pipeline]($./02-Generic_dlt_meta_data_pipeline)**  from the current directory.
# MAGIC 6. For **Destination**, choose **Catalog = dlt_catalog**, **Schema = dlt_meta**.
# MAGIC 7. Populate advanced configuration as 
# MAGIC     `layer` : `bronze_silver`, `bronze.group`: `A1`, `bronze.dataflowspecTable`: `bronze dataflowspec table name (here table name is dlt_catalog.dlt_meta.bronze_specs)`, `silver.group`: `A1`, `silver.dataflowspecTable`: `bronze dataflowspec table name (here table name is dlt_catalog.dlt_meta.silver_specs)`, `version`: `0.0.9`
# MAGIC
# MAGIC 8. Uncheck **Enable autoscaling**, set **Workers = 1**.
# MAGIC 9. In **Advanced settings**, choose a worker type with *8GB RAM & 2 cores*.
# MAGIC 10. Check **Publish event log to Unity Catalog** option, and provide **Catalog**, **Schema** and **Event log table name** details.
# MAGIC 11. Click **Create**.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###5.1 Run the Pipeline
# MAGIC
# MAGIC Now run your DLT pipeline:
# MAGIC
# MAGIC 1. Select **Development** mode for faster runs and easier debugging.  
# MAGIC    * It reuses the cluster and skips retries to speed up iteration.  
# MAGIC    * Learn more in the [documentation](https://docs.databricks.com/aws/en/dlt/develop).
# MAGIC 2. Click **Start**.
# MAGIC
# MAGIC First run may take a few minutes to provision the cluster.  
# MAGIC Later runs will be much faster.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###5.2 Exploring the Results of a DLT Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ###What Just Happened?
# MAGIC
# MAGIC After running the pipeline, these assets were created in Unity Catalog:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Bronze Layer - Raw Ingestion  
# MAGIC **Catalog**: `dlt_catalog`  
# MAGIC **Schema**: `dlt_meta`  
# MAGIC **Tables**: `bronze_customer`, `bronze_product`, `bronze_transaction`, `customer_quarantine`, `product_quarantine`, `transaction_quarantine`  
# MAGIC
# MAGIC Raw data as ingested from source files.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Silver Layer - Cleaned & Validated  
# MAGIC **Catalog**: `dlt_catalog`  
# MAGIC **Schema**: `dlt_meta`  
# MAGIC **Tables**: `silver_customer`, `silver_product`, `silver_transaction` 
# MAGIC
# MAGIC Cleaned data with data quality rules applied.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###6. The DLT DAG shows how data flows between tables in your pipeline — from Bronze to Silver
# MAGIC
# MAGIC
# MAGIC
# MAGIC - **Bronze and Silver layer DAG:**
# MAGIC
# MAGIC ![](/Workspace/Users/maheshwar.kasireddy@impetus.com/DLT_META_USE_CASE/include/bronze_silver_layer_dag.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##6.1 DLT Event Log
# MAGIC
# MAGIC **Uncheck  `Publish event log to Unity Catalog` option**:
# MAGIC - DLT automatically creates a **hidden table** called the **event log**.
# MAGIC - It stores all pipeline events: table creation, data quality results, errors, and run status.
# MAGIC - Located in your pipeline's **catalog and schema**, as:
# MAGIC   ```
# MAGIC   <catalog>.<schema>.event_log__<pipeline_id>
# MAGIC   ```
# MAGIC - **Only the pipeline owner** can query it.
# MAGIC - Use query to explore it manually (not visible in data explorer).
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Check  `Publish event log to Unity Catalog` option**:
# MAGIC - DLT automatically created event log table based catalog, Schema and table name provided information
# MAGIC - It stores all pipeline events: table creation, data quality results, errors, and run status.
# MAGIC - Located in your **catalog and schema**, as:
# MAGIC   ```
# MAGIC   <catalog>.<schema>.<Event_log_table_name>
# MAGIC   ```

# COMMAND ----------

# MAGIC %md
# MAGIC ###Query the event log and analyze your pipeline execution:

# COMMAND ----------

# MAGIC %md
# MAGIC ####Bronze and silver layer Pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC SELECT
# MAGIC   row_expectations.dataset as dataset,
# MAGIC   row_expectations.name as expectation,
# MAGIC   SUM(row_expectations.passed_records) as passing_records,
# MAGIC   SUM(row_expectations.failed_records) as failing_records,
# MAGIC   update_id
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(
# MAGIC         from_json(
# MAGIC           details:flow_progress.data_quality.expectations,
# MAGIC           "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
# MAGIC         )
# MAGIC       ) row_expectations,
# MAGIC       origin.update_id as update_id
# MAGIC     FROM
# MAGIC       dlt_catalog.dlt_meta.event_log_dlt_meta_bronze_silver
# MAGIC     WHERE
# MAGIC       event_type = 'flow_progress'
# MAGIC )
# MAGIC GROUP BY
# MAGIC   row_expectations.dataset,
# MAGIC   row_expectations.name,
# MAGIC   update_id )
# MAGIC SELECT * FROM cte WHERE update_id = (SELECT origin.update_id AS id FROM dlt_catalog.dlt_meta.event_log_dlt_meta_bronze_silver ORDER BY timestamp DESC limit 1 )

# COMMAND ----------

# MAGIC %md
# MAGIC ###7. Row and Column level security
# MAGIC
# MAGIC Refer below notebook for Row and column level security implementation
# MAGIC
# MAGIC [Row and Column level security]($./04_row_column_level_security)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Run below command to cleanup

# COMMAND ----------

cleanup_data(spark,CATALOG,DLT_META_DATABASE )