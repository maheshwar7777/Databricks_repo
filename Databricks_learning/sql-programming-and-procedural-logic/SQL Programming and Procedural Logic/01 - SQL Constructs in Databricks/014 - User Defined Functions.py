# Databricks notebook source
# MAGIC %md
# MAGIC # User Defined Functions
# MAGIC In this demo, we will learn how to create and use SQL User Defined Functions (UDFs) in Databricks SQL to add custom logic to your queries. By working through practical steps with TPCH sample data, you'll define scalar UDFs and apply them to real business problems, such as classifying the line items into bulk, standard or small based on quantity value. By the end of this demo, you will understand how SQL UDFs help extend analytic functionality and make your SQL logic reusable and dynamic in Databricks.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC * Apply SQL user defined functions to encapsulate custom logic in Databricks SQL
# MAGIC * Execute queries using scalar user defined functions to process and classify data
# MAGIC * Perform common table expression operations that leverage user-defined functions on TPCH tables
# MAGIC * Demonstrate multi-condition business rules within SQL user defined function logic

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
# MAGIC * **CREATE FUNCTION** privilege to define SQL User Defined Functions (UDFs) in your catalog/schema
# MAGIC * **CREATE CATALOG** and **USE CATALOG** privileges in your metastore to create and manage catalogs
# MAGIC * **CREATE SCHEMA** and **USE SCHEMA** privileges in your metastore to manage schemas
# MAGIC * **SELECT** privileges on relevant TPCH tables \(such as orders and lineitem\) to perform queries

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
# MAGIC ## 🧮 Exercise: SQL User-Defined Functions (UDFs)
# MAGIC
# MAGIC **Goal:** Write and use SQL scalar functions to encapsulate and reuse row-level logic.
# MAGIC
# MAGIC In this activity, we will:
# MAGIC 1. Define a boolean UDF, `is_large_lineitem`, that returns `TRUE` for line item quantity values greater than 100.
# MAGIC 2. Define a second, more complex UDF that classifies line items into three categories, based on quantity value:
# MAGIC    - `"bulk"` for quantity values greater than 35
# MAGIC    - `"standard"` for quantity values between 21 and 35
# MAGIC    - `"small"` for quantity values less than or equal to 20
# MAGIC 3. Use your new UDFs in a query to count the number of each line item type.
# MAGIC
# MAGIC > 💡 SQL UDFs allow you to centralize logic and reuse it consistently across dashboards, queries, and pipelines.
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
# MAGIC Let us start by defining a boolean user-defined function called `is_large_lineitem`. This function takes a quantity value as input and returns `TRUE` if the quantity is greater than 100, otherwise it returns `FALSE`. With this function, we can easily identify which line items represent large quantity orders.
# MAGIC
# MAGIC Run the below cell to perform this task:

# COMMAND ----------

# DBTITLE 1,ANSWER: SQL UDFs
# MAGIC %sql
# MAGIC ---- Step 1: Create UDF is_large_lineitem(qty) → returns TRUE if quantity > 100
# MAGIC CREATE OR REPLACE FUNCTION is_large_lineitem(qty DOUBLE)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN qty > 100;

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2:**
# MAGIC
# MAGIC
# MAGIC Next, let us create another user-defined function called `lineitem_size_label`. This function categorizes line items based on their quantity. If the quantity is **greater than 35**, the function labels it as `'bulk'`. If the quantity falls **between 21 and 35**, it is classified as `'standard'`. For quantities of **20 or less**, the label `'small'` is assigned. This labeling makes it simple to group and analyze line items by size category.
# MAGIC
# MAGIC Run the below cell to perform this task:

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Step 2: Create a second UDF to return 'bulk', 'standard', or 'small' based on quantity
# MAGIC CREATE OR REPLACE FUNCTION lineitem_size_label(qty DOUBLE)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN qty > 35 THEN 'bulk'
# MAGIC   WHEN qty BETWEEN 21 AND 35 THEN 'standard'
# MAGIC   ELSE 'small'
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3:**
# MAGIC
# MAGIC
# MAGIC Finally, let us apply the `lineitem_size_label` user-defined function to the `l_quantity` column of the `lineitem` table. In this query, we use the function to classify each line item, then group the results by these categories. We count the number of line items in each category and sort the results in descending order, so we can see which size category has the highest number of orders. This aggregation provides a clear summary of the distribution of order sizes within our dataset.
# MAGIC
# MAGIC Run the below cell to perform this task:

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Step 3: Use your lineitem_size_label(qty) UDF in a GROUP BY query to count each classification
# MAGIC SELECT
# MAGIC   lineitem_size_label(l_quantity) AS size_category,
# MAGIC   COUNT(*) AS num_orders
# MAGIC FROM ${DA.user_catalog_name}.bronze.lineitem
# MAGIC GROUP BY size_category
# MAGIC ORDER BY num_orders DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up
# MAGIC Run the below cell to delete the custom user-defined function named `is_large_lineitem`:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete the is_large_lineitem user-defined function
# MAGIC DROP FUNCTION IF EXISTS is_large_lineitem;

# COMMAND ----------

# MAGIC %md
# MAGIC Run the below cell to delete the custom user-defined function named `lineitem_size_label`:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete the lineitem_size_label user-defined function
# MAGIC DROP FUNCTION IF EXISTS lineitem_size_label;

# COMMAND ----------

# MAGIC %md
# MAGIC Run the below cell to delete the unique catalog `DA.user_catalog_name` created for this user:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the catalog with CASCADE
# MAGIC -- This removes all schemas/tables within it as well.
# MAGIC DROP CATALOG IF EXISTS ${DA.user_catalog_name} CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC In this demo, we built custom SQL User Defined Functions in Databricks to solve real data analysis challenges. We practiced defining scalar UDFs, calling them in queries, and combining them with CTEs to classify orders as small, bulk, or standard. Mastering UDFs enables you to perform advanced data transformations and tailor SQL workflows to your organization’s unique business logic in Databricks.