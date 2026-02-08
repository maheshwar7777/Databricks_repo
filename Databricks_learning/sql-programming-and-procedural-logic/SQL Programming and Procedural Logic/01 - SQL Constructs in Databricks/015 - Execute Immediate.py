# Databricks notebook source
# MAGIC %md
# MAGIC # Execute Immediate
# MAGIC In this demo, we will learn how to create and use dynamic SQL in Databricks using the `EXECUTE IMMEDIATE` statement. Through a series of hands-on steps, you’ll declare and manipulate variables, inject them into SQL strings, and conditionally control query logic at runtime. By the end of this demo, you will know how to leverage dynamic SQL to make your queries flexible and adaptable to various analysis scenarios.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC * Apply variables to create flexible SQL logic in Databricks
# MAGIC * Execute dynamic queries using the EXECUTE IMMEDIATE statement
# MAGIC * Demonstrate conditional logic by injecting variables into SQL strings
# MAGIC * Perform runtime query customization for data filtering and sorting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC In order to follow along with this demo, you will need:
# MAGIC * Cloud resources to support the metastore
# MAGIC * Access to a Unity-Catalog enabled Databricks workspace with the ability to create catalogs in your metastore
# MAGIC * Permission to query TPCH sample datasets or equivalent data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC To run this notebook, you need to meet the following technical considerations:
# MAGIC
# MAGIC * Databricks runtime\(s\): **16.4.x-scala2.13**. **Do NOT use serverless compute to run this notebook**.
# MAGIC * **Unity Catalog** enabled workspace
# MAGIC * Access to the **TPCH sample datasets** or equivalent data within your workspace
# MAGIC * **CREATE CATALOG** and **USE CATALOG** privileges in your metastore to create and manage catalogs
# MAGIC * **CREATE SCHEMA** and **USE SCHEMA** privileges in your metastore to manage schemas
# MAGIC * **SELECT** privileges on TPCH tables \(customer, orders, nation, region\) to perform queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**. <br>
# MAGIC
# MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC     - In the drop-down, select **More**.
# MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC 3. Wait a few minutes for the cluster to start.
# MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script. This script will automatically define configuration variables necessary for the demo and create a personalized, secure data environment for each user in Databricks using Unity Catalog's three-level namespace ensuring isolation, modularity, and easy organization for data workflows.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Core Functionality:
# MAGIC * **Creates a DA object** to refer to major variables required for this notebook.
# MAGIC * **Generates a unique catalog** for each user, based on their identity.
# MAGIC * **Creates three schemas** within the catalog: `bronze`, `silver`, and `gold`, following the common lakehouse multi-layer architecture.
# MAGIC * **Copies TPCH sample data tables** into the `bronze` schema for hands-on exercises or further processing.
# MAGIC * **Applies data access controls**, so only the respective user can see or use their catalog and schemas.
# MAGIC
# MAGIC ### Structure Created:
# MAGIC 1. **Catalog**
# MAGIC     * A user-specific top-level container.
# MAGIC
# MAGIC 2. **Schemas**
# MAGIC     * **bronze:** Contains base data tables \(e.g., TPCH tables: `customer`, `orders`, etc.\).
# MAGIC     * **silver:** Created empty; intended for refined/curated datasets.
# MAGIC     * **gold:** Created empty; intended for business-level aggregations and reporting.
# MAGIC
# MAGIC 3. **Tables**
# MAGIC     * All tables from the `samples.tpch` source database are replicated into the user's bronze schema.
# MAGIC
# MAGIC ### Hierarchical Layout
# MAGIC The final layout of the structure generate by this script in your unity catalog environment is as shown below:
# MAGIC ```html
# MAGIC [Catalog: <user_catalog_name>]
# MAGIC ├── [Schema: bronze]
# MAGIC │   ├── customer
# MAGIC │   ├── lineitem
# MAGIC │   ├── nation
# MAGIC │   ├── orders
# MAGIC │   ├── part
# MAGIC │   ├── partsupp
# MAGIC │   ├── region
# MAGIC │   └── supplier
# MAGIC ├── [Schema: silver]
# MAGIC └── [Schema: gold]
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Other Conventions:
# MAGIC
# MAGIC Throughout this demo, we'll refer to the object `DA`. This object, contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

# COMMAND ----------

# List the DA object components
print(f"Username:                           {DA.username}")
print(f"Default Catalog Name:               {DA.catalog_name}")
print(f"Default Schema Name:                {DA.schema_name}")
print(f"Warehouse ID:                       {DA.warehouse_id}")
print(f"Warehouse Name:                     {DA.warehouse_name}")
print(f"Auto-Generated User Catalog Name:   {DA.user_catalog_name}")
print(f"User Home Directory Path:           {DA.user_home_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise: EXECUTE IMMEDIATE – Dynamic SQL
# MAGIC
# MAGIC **Goal:** Construct SQL statements dynamically at runtime using variable input.
# MAGIC
# MAGIC In this activity, we will:
# MAGIC 1. Declare a variable for a target year (e.g., 1996).
# MAGIC 2. Use `EXECUTE IMMEDIATE` to query total revenue and order count for that year.
# MAGIC 3. Add a second variable for a sort column, and dynamically apply an ORDER BY clause.
# MAGIC
# MAGIC > 💡 Use `EXECUTE IMMEDIATE` when your logic needs to adapt to runtime inputs or when building flexible, parameter-driven notebooks and workflows.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Run the below cell to set your default catalog to the unique user catalog created for your account and set the default schema to the bronze schema within that catalog:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set current catalog and schema for this user/session
# MAGIC USE CATALOG ${DA.user_catalog_name};
# MAGIC USE SCHEMA `bronze`;

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1:**
# MAGIC
# MAGIC
# MAGIC Let us start by declaring two variables that will be used to parameterize our analysis. In this cell, we define `target_year` as an integer with a default value of **1996**. We also define `sort_column` as a string, initializing it with the value `'total_revenue'`. These variables allow us to set which year's data to analyze and choose the column by which our results will be sorted.
# MAGIC
# MAGIC Run the below cell to perform this task:

# COMMAND ----------

# DBTITLE 1,ANSWER: EXECUTE IMMEDIATE
# MAGIC %sql
# MAGIC ---- Step 1: Declare a variable for a target year
# MAGIC DECLARE OR REPLACE target_year INT DEFAULT 1996;
# MAGIC DECLARE OR REPLACE sort_column STRING DEFAULT 'total_revenue';

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2:**
# MAGIC
# MAGIC
# MAGIC Next, we use `EXECUTE IMMEDIATE` to construct and run a SQL query that summarizes order data for the specified year. The query creates a summary table with order priority, the number of orders, and total revenue, filtering by `target_year`. It then selects all results from the summary and sorts them in descending order based on the column name specified by `sort_column`, giving us a dynamic and flexible summary based on our chosen parameters.
# MAGIC
# MAGIC Run the below cell to perform this task:

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Step 2: Use EXECUTE IMMEDIATE to construct the full query
# MAGIC ---- 2.1 Dynamically query number of orders and total revenue for that year
# MAGIC ---- 2.2 Add a variable to specify a sort column (e.g., o_totalprice) and ORDER BY it
# MAGIC EXECUTE IMMEDIATE "
# MAGIC   WITH revenue_summary AS (
# MAGIC     SELECT
# MAGIC       o_orderpriority,
# MAGIC       COUNT(*) AS num_orders,
# MAGIC       SUM(o_totalprice) AS total_revenue
# MAGIC     FROM ${DA.user_catalog_name}.bronze.orders
# MAGIC     WHERE year(o_orderdate) = ?
# MAGIC     GROUP BY o_orderpriority
# MAGIC   )
# MAGIC   SELECT *
# MAGIC   FROM revenue_summary
# MAGIC   ORDER BY ? DESC"
# MAGIC   USING target_year, sort_column;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up
# MAGIC Run the below cell to delete the unique catalog `DA.user_catalog_name` created for this user:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the catalog with CASCADE
# MAGIC -- This removes all schemas/tables within it as well.
# MAGIC DROP CATALOG IF EXISTS ${DA.user_catalog_name} CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC In this demo, we used `EXECUTE IMMEDIATE` in Databricks SQL to build dynamic, flexible queries by combining variables and SQL string construction. We practiced injecting conditions and sorting dynamically, and saw how conditional logic enables tailored result sets. With these techniques, you can streamline complex analysis, re-use logic, and respond to changing requirements directly in your SQL workflows.