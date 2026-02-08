# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting Started with Lakeflow Declarative Pipelines
# MAGIC
# MAGIC Delta Live Tables (DLT) are now enhanced under the **Lakeflow Declarative Pipelines** umbrella in Databricks. These pipelines allow data engineers to define **data workflows as code** using SQL or Python and manage data quality, lineage, and infrastructure seamlessly.
# MAGIC
# MAGIC ### Key Features:
# MAGIC - **Declarative syntax** for building ETL pipelines.
# MAGIC - **Managed DAG** with data lineage and metrics.
# MAGIC - **Automatic schema evolution** and **data quality enforcement**.
# MAGIC - **Pipeline orchestration** with **Triggered** and **Continuous** modes.
# MAGIC
# MAGIC More details: [Databricks Product Page](https://docs.databricks.com/aws/en/dlt/)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC 1. Review pipeline configurations [config.py]($./Includes/config.py)
# MAGIC 2. Generate sample data using  [Data-Generator]($./Includes/Data-Generator)
# MAGIC 3. DLT Logic Notebook [01-Python for Delta Live Tables]($./01-Python for Delta Live Tables)
# MAGIC 3. Create and run a Delta Live Table pipeline 
# MAGIC 5. Explore DLT DAG and event logs
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Setup  
# MAGIC Requires 'Create Catalog' and 'Create Cluster' permissions.  
# MAGIC Run the cell below to set up demo data.

# COMMAND ----------

from Includes.config import * 
from Includes.dlt_utility import * 

# COMMAND ----------

# Dropdown widget to select workspace type
dbutils.widgets.dropdown("workspace_type", "Trial Edition", ["Trial Edition", "14 days Trial", "Premium or Enterprise"], "Workspace Type")
workspace_type = dbutils.widgets.get("workspace_type")

# COMMAND ----------

dbutils.notebook.run("./Includes/data_generator", 200)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Run the cell below to view config values.

# COMMAND ----------

print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create DLT Pipeline
# MAGIC
# MAGIC Choose one of the two options to create your Delta Live Table (DLT) pipeline:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Option A: Auto-Creation (Recommended)**  
# MAGIC Run `create_pipeline()` below to:  
# MAGIC - Auto-create the DLT pipeline  
# MAGIC - Set paths, cluster, and notebook  
# MAGIC - No UI steps needed  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Option B: Manual Creation (Optional)**  
# MAGIC Follow the UI steps below to manually set up your pipeline in Databricks.
# MAGIC

# COMMAND ----------

# Option A
pipeline_id = create_pipeline("DLT_demo_pipeline", workspace_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B: Create Pipeline Manually via UI
# MAGIC
# MAGIC To create your DLT pipeline in Lakeview Declarative Pipelines:
# MAGIC
# MAGIC 1. Go to **Jobs & Pipelines** > click **New > ETL Pipeline**.
# MAGIC 2. Enter a unique **Pipeline name**.
# MAGIC 3. Set **Product Edition** to **Advanced** (default).
# MAGIC 4. **Uncheck Serverless**.
# MAGIC 5. Set **Pipeline mode** to **Triggered** (uses AvailableNow trigger).
# MAGIC 6. Under **Notebook Libraries**, select the notebook named **[Python for Delta Live Tables]($./Python for Delta Live Tables)**  from the current directory.
# MAGIC 7. For **Destination**, choose **Catalog = DLT_demo_{username}**, **Schema = default**.
# MAGIC 8. Uncheck **Enable autoscaling**, set **Workers = 1**.
# MAGIC 9. In **Advanced settings**, choose a worker type with *8GB RAM & 2 cores*.
# MAGIC 10. Click **Create**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Pipeline
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
# MAGIC
# MAGIC
# MAGIC ### Exploring the Results of a DLT Pipeline
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### What Just Happened?
# MAGIC
# MAGIC After running the pipeline, these assets were created in Unity Catalog:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Bronze Layer - Raw Ingestion  
# MAGIC **Catalog**: `DLT_demo_{username}`  
# MAGIC **Schema**: `BRONZE_DATABASE`  
# MAGIC **Tables**: `encounters`, `organizations`, `patients`  
# MAGIC Raw data as ingested from source files.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Silver Layer - Cleaned & Validated  
# MAGIC **Catalog**: `DLT_demo_{username}`  
# MAGIC **Schema**: `SILVER_DATABASE`  
# MAGIC **Tables**: `encounters`, `organizations`, `patients`  
# MAGIC Cleaned data with data quality rules applied.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Gold Layer - Monthly Org Metrics  
# MAGIC **Catalog**: `DLT_demo_{username}`  
# MAGIC **Schema**: `GOLD_DATABASE`  
# MAGIC **Table**: `gold_org_monthly_metrics`
# MAGIC
# MAGIC **Columns**:
# MAGIC - `organization`, `organization_name`, `encounter_month`, `monthly_cost`, `unique_patients`, `total_visits`, `avg_cost_per_visit`
# MAGIC
# MAGIC Purpose: Aggregated KPIs for org-level monthly reporting.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###The DLT DAG shows how data flows between tables in your pipeline — from Bronze to Silver to Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/prachi.sankhala@impetus.com/DLT_series/Includes/pipeline_dag.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Event Log
# MAGIC
# MAGIC - DLT automatically creates a **hidden table** called the **event log**.
# MAGIC - It stores all pipeline events: table creation, data quality results, errors, and run status.
# MAGIC - Located in your pipeline's **catalog and schema**, as:
# MAGIC   ```
# MAGIC   <catalog>.<schema>.event_log__<pipeline_id>
# MAGIC   ```
# MAGIC - **Only the pipeline owner** can query it.
# MAGIC - Use query to explore it manually (not visible in data explorer).
# MAGIC

# COMMAND ----------

#create view on top of event log for query
df = spark.sql(f"select * from {CATALOG}.default.event_log_{pipeline_id.replace('-','_')}")

df.createOrReplaceTempView("event_log_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC This is how you can query the event log and analyze your pipeline execution:

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
# MAGIC       event_log_raw
# MAGIC     WHERE
# MAGIC       event_type = 'flow_progress'
# MAGIC )
# MAGIC GROUP BY
# MAGIC   row_expectations.dataset,
# MAGIC   row_expectations.name,
# MAGIC   update_id )
# MAGIC SELECT * FROM cte WHERE update_id = (SELECT origin.update_id AS id FROM event_log_raw ORDER BY timestamp DESC limit 1 )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  
# MAGIC Run the following cell to delete pipeline, tables and files associated with this lesson.

# COMMAND ----------

cleanup_data(pipeline_id)