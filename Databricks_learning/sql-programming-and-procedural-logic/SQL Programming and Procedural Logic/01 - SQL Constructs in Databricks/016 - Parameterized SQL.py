# Databricks notebook source
# MAGIC %md
# MAGIC # Parameterized SQL
# MAGIC In this demo, we will learn how to use parameterized SQL in Databricks by leveraging SQL widgets to build interactive and reusable queries. Through practical steps, you'll add widgets for user-driven input, incorporate those parameters in your SQL with safe notation, and use both single-value and multi-select widgets to filter and customize results dynamically. By the end of this demo, you will be able to create flexible, user-friendly SQL analytics workflows in Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC * Apply SQL widgets to capture user inputs for parameterized queries in Databricks
# MAGIC * Execute queries using embedded parameters for dynamic filtering
# MAGIC * Demonstrate the use of both single-value and multi-select widgets in SQL
# MAGIC * Perform real-time query customization through interactive widgets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC In order to follow along with this demo, you will need:
# MAGIC * Cloud resources to support the metastore.
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
# MAGIC ## 🚨REQUIRED - SELECT CLASSIC COMPUTE
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
# MAGIC ## 🎛️ Exercise: SQL Widgets – Interactive Inputs for Parameterized Queries
# MAGIC
# MAGIC **Goal:** Use SQL widgets to enable dynamic, user-driven query behavior in notebooks.
# MAGIC
# MAGIC In this activity, you will:
# MAGIC 1. Create a `TEXT` widget for selecting a year.
# MAGIC 2. Create a `TEXT` or `DROPDOWN` widget for specifying a minimum order quantity.
# MAGIC 3. Create a `MULTISELECT` widget for filtering by product category.
# MAGIC 4. Use all widget inputs to build a parameterized report of quantity sold by category.
# MAGIC
# MAGIC > 💡 SQL widgets integrate with Databricks workflows, allowing dynamic parameter injection into notebooks and pipelines.
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
# MAGIC Let us begin by creating a widget to capture the year for our analysis. We will start by defining a `TEXT` widget named `year_input` and set its default value to '1996'. This widget allows users to specify the reporting year directly from an interactive control within the notebook.
# MAGIC
# MAGIC Run the below cell to perform this task:

# COMMAND ----------

# DBTITLE 1,ANSWER: Create SQL Widgets
# MAGIC %sql
# MAGIC ---- Step 1: Create a TEXT widget for year, 'year_input'
# MAGIC CREATE WIDGET TEXT year_input DEFAULT '1996';

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2:**
# MAGIC
# MAGIC
# MAGIC Next, we will create another input for specifying minimum order quantity. Here, we define a `TEXT` widget called `min_qty_input` with a default value of '10'. This enables users to dynamically set a threshold for the minimum quantity to include in their query results.
# MAGIC
# MAGIC Run the below cell to perform this task:

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Step 2: Create a TEXT widget for minimum quantity ,'min_qty_input'
# MAGIC CREATE WIDGET TEXT min_qty_input DEFAULT '10';

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3:**
# MAGIC
# MAGIC
# MAGIC Let us now enhance interactivity by providing a way to filter results by product manufacturer. We use a `MULTISELECT` widget named `mfgr_input` and initialize it with **"Manufacturer#1"** as the default selection. The available choices are a set of predefined manufacturer names. This widget lets users select one or more manufacturers to focus the analysis on specific product categories.
# MAGIC
# MAGIC Run the below cell to perform this task:

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Step 3: Create a MULTISELECT widget based on an array of CHOICES for product manufacturers, 'mfgr_input'
# MAGIC CREATE WIDGET MULTISELECT mfgr_input DEFAULT "Manufacturer#1" CHOICES (SELECT * FROM VALUES ("Manufacturer#1"), ("Manufacturer#2"), ("Manufacturer#3"), ("Manufacturer#4"), ("Manufacturer#5"));

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4:**
# MAGIC
# MAGIC
# MAGIC Finally, we will now combine the user inputs from all widgets to build a parameterized SQL query that reports the total quantity sold by manufacturer, filtered according to the selected year, minimum quantity, and chosen manufacturers. This approach makes the analysis interactive and user-driven, supporting a flexible reporting experience.
# MAGIC
# MAGIC Run the below cell to perform this task:

# COMMAND ----------

# DBTITLE 1,ANSWER: Query SQL Widgets
# MAGIC %sql
# MAGIC ---- Step 4: Query using all widget values
# MAGIC ---- Note: Use array_contains() and split() for multiselect filtering
# MAGIC SELECT
# MAGIC   p.p_mfgr,
# MAGIC   SUM(l.l_quantity) AS total_quantity
# MAGIC FROM ${DA.user_catalog_name}.bronze.lineitem l
# MAGIC JOIN ${DA.user_catalog_name}.bronze.part p ON l.l_partkey = p.p_partkey
# MAGIC JOIN ${DA.user_catalog_name}.bronze.orders o ON l.l_orderkey = o.o_orderkey
# MAGIC WHERE year(o.o_orderdate) = CAST(:year_input AS INT)
# MAGIC   AND l.l_quantity >= CAST(:min_qty_input AS INT)
# MAGIC   AND array_contains(split(:mfgr_input, ','), p.p_mfgr)
# MAGIC GROUP BY p.p_mfgr
# MAGIC ORDER BY total_quantity DESC;

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
# MAGIC Run the below cell to delete all the widgets created for notebook:

# COMMAND ----------

# Delete all widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC In this demo, we used Databricks widgets to parameterize SQL queries, enabling dynamic filtering and selection directly in notebooks. We practiced creating text and multi-select widgets, incorporated user input for real-time analysis, and made our SQL queries adaptable for various end users. Mastering SQL parameterization allows you to make analytics more interactive, reusable, and tailored to business needs in Databricks.