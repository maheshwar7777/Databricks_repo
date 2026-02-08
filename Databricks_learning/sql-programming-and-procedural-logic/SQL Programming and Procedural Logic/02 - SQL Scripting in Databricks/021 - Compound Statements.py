# Databricks notebook source
# MAGIC %md
# MAGIC # Compound Statements
# MAGIC In this demo, we will learn how to use compound statements in Databricks SQL scripting to build modular, multi-step workflows. By encapsulating logic within `BEGIN` and `END` blocks, you’ll execute several related SQL operations as a single unit, manage variable scope, and create chained temp views for rich data analysis. By the end of this demo, you will be able to structure and sequence SQL tasks for more organized and reusable scripts in Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC * Apply compound statements to organize multi-step SQL workflows in Databricks
# MAGIC * Execute sequential SQL operations within `BEGIN...END` blocks
# MAGIC * Demonstrate the use of scoped variables and temporary views in scripting
# MAGIC * Perform data refinement by chaining and reusing results within a compound block

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
# MAGIC * **Serverless** compute enabled
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
# MAGIC Throughout this demo, we'll refer to the object `DA`. This object,contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

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
# MAGIC ## 🧱 Exercise: Compound Statements
# MAGIC
# MAGIC **Goal:** Use a Compound Statement to Filter and Join
# MAGIC
# MAGIC In this activity, we will create a SQL script that:
# MAGIC
# MAGIC 1. Filters the `orders` table to include only orders with `o_totalprice` less than 10,000.
# MAGIC 2. Joins this subset with the `customer` table to retrieve the customer's region (from `c_nationkey` join via `nation` table).
# MAGIC 3. Outputs a summary of the order ID, total price, and the customer's region.

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
# MAGIC **Task Details:**
# MAGIC
# MAGIC * **Step 1:**
# MAGIC
# MAGIC     * Let us begin the first step, by creating a temporary view called `low_value_orders`. Here, we select order keys, customer keys, and total prices from the orders table, but only include orders with a total price less than 10,000. This effectively filters out higher-value orders, focusing the analysis on smaller transactions.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC
# MAGIC         ```sql
# MAGIC         ---- Step 1: Filter low-value orders
# MAGIC         CREATE OR REPLACE TEMP VIEW low_value_orders AS
# MAGIC         SELECT o_orderkey, o_custkey, o_totalprice
# MAGIC         FROM ${DA.user_catalog_name}.bronze.orders
# MAGIC         WHERE o_totalprice < 10000;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 2:**
# MAGIC
# MAGIC     * Next, we define another temporary view named `customer_region`. This step joins the customer table with the nation table to associate each customer with their nation and region. We select customer keys, customer names, nation names, and region keys, so we can later report on orders by regional breakdown.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         ---- Step 2: Join with customer and nation to get region
# MAGIC         CREATE OR REPLACE TEMP VIEW customer_region AS
# MAGIC         SELECT c.c_custkey, c.c_name AS customer_name, n.n_name AS nation_name, n.n_regionkey as regionkey
# MAGIC         FROM ${DA.user_catalog_name}.bronze.customer c
# MAGIC         JOIN ${DA.user_catalog_name}.bronze.nation n ON c.c_nationkey = n.n_nationkey;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 3:**
# MAGIC
# MAGIC     * Finally, we bring the results together by joining `low_value_orders` with `customer_region` on the customer key. The query selects the order key, total price, and region for each qualifying order. This produces a summary linking low-value orders with customer and regional information, making it easy to understand where smaller transactions are occurring across different regions.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         ---- Step 3: Final join to summarize order data
# MAGIC         SELECT o.o_orderkey, o.o_totalprice, cr.regionkey
# MAGIC         FROM low_value_orders o
# MAGIC         JOIN customer_region cr ON o.o_custkey = cr.c_custkey;
# MAGIC         ```
# MAGIC
# MAGIC
# MAGIC Finally, we will organize all three steps within a `BEGIN...END` block to run the operations together as a single logical unit.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Implimentation Instructions
# MAGIC
# MAGIC ##### 1. Open the SQL Editor 📝
# MAGIC - In the left-hand Databricks sidebar, right-click on **SQL Editor** and select the **"Open Link in New Tab"** option.
# MAGIC
# MAGIC ##### 2. Paste and Update Your SQL Code ⌨️
# MAGIC - In the new, blank Query tab, paste the following SQL code *(make sure to update the catalog as instructed)*:
# MAGIC
# MAGIC   ```sql
# MAGIC   -- Use BEGIN...END blocks
# MAGIC   -- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC   BEGIN
# MAGIC     -- Step 1: Filter low-value orders
# MAGIC     CREATE OR REPLACE TEMP VIEW low_value_orders AS
# MAGIC     SELECT o_orderkey, o_custkey, o_totalprice
# MAGIC     FROM ${DA.user_catalog_name}.bronze.orders
# MAGIC     WHERE o_totalprice < 10000;
# MAGIC
# MAGIC     -- Step 2: Join with customer and nation to get region
# MAGIC     CREATE OR REPLACE TEMP VIEW customer_region AS
# MAGIC     SELECT c.c_custkey, c.c_name AS customer_name, n.n_name AS nation_name, n.n_regionkey as regionkey
# MAGIC     FROM ${DA.user_catalog_name}.bronze.customer c
# MAGIC     JOIN ${DA.user_catalog_name}.bronze.nation n ON c.c_nationkey = n.n_nationkey;
# MAGIC
# MAGIC     -- Step 3: Final join to summarize order data
# MAGIC     SELECT o.o_orderkey, o.o_totalprice, cr.regionkey
# MAGIC     FROM low_value_orders o
# MAGIC     JOIN customer_region cr ON o.o_custkey = cr.c_custkey;
# MAGIC   END;
# MAGIC   ```
# MAGIC
# MAGIC ##### 3. Replace the Catalog Name 🔗
# MAGIC
# MAGIC - Update `${DA.user_catalog_name}` with your assigned catalog name to ensure the query references your data properly.
# MAGIC
# MAGIC ##### 4. Remove the Row Limit
# MAGIC - Before running and saving, ensure there are no row result limits:
# MAGIC   - Click the dropdown next to the **Run** button in the query editor.
# MAGIC   - Uncheck the row limit (default is 1,000 rows) for complete results in automated jobs.
# MAGIC
# MAGIC ##### 5. Run Query ▶️
# MAGIC
# MAGIC - Click the **Run** button to execute your query:
# MAGIC
# MAGIC
# MAGIC ##### Final Output Preview 🖥️
# MAGIC
# MAGIC Once you have lifted the row limit and run your query, your output will look like this:
# MAGIC   ![Query 1 - Output](../Includes/images/compound_statements.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up
# MAGIC Run the below cell to delete the temporary view `low_value_orders` created for your current session:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete the temp view for low-value orders
# MAGIC DROP VIEW IF EXISTS low_value_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC Run the below cell to delete the temporary view `customer_region` created for your current session:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete the temp view for customer and region mapping
# MAGIC DROP VIEW IF EXISTS customer_region;

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
# MAGIC In this demo, we used compound statements in Databricks SQL to group related operations within executable blocks, chaining together temporary views and queries for stepwise data refinement. By structuring logic in compound statements, you can manage complexity, control execution order, and create clear, maintainable SQL scripts for sophisticated analyses.