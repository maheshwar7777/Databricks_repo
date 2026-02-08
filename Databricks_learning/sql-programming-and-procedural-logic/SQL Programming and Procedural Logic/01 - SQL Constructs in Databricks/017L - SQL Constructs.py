# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Constructs
# MAGIC
# MAGIC In this lab, we will explore core SQL programming constructs in Databricks using TPCH sample data to build, organize, and streamline analytical workflows. Through a series of practical exercises, you will use Common Table Expressions for modular query design, create temporary and global temporary views for reusable intermediate results, define SQL User Defined Functions for custom business logic, execute dynamic SQL at runtime, and apply parameterized SQL for interactive analytics. By the end of this lab, you will be able to write cleaner, more maintainable, and dynamic SQL code for real-world data analysis in Databricks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC * Apply common table expressions to aggregate, rank, and analyze supplier data
# MAGIC * Execute temporary and global temporary views for reusable intermediate results
# MAGIC * Demonstrate SQL user defined functions to encapsulate and reuse business logic
# MAGIC * Perform dynamic SQL execution with variables using `EXECUTE IMMEDIATE`
# MAGIC * Apply parameterized SQL with widgets for interactive data filtering

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC In order to follow along with this lab, you will need:
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
# MAGIC Before starting the lab, run the provided classroom setup script. This script will automatically define configuration variables necessary for the lab and create a personalized, secure data environment for each user in Databricks using Unity Catalog's three-level namespace ensuring isolation, modularity, and easy organization for data workflows.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Other Conventions:
# MAGIC
# MAGIC Throughout this lab, we'll refer to the object `DA`. This object, contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

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
# MAGIC Run the below cell to set your default catalog to the unique user catalog created for your account and set the default schema to the bronze schema within that catalog:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set current catalog and schema for this user/session
# MAGIC USE CATALOG ${DA.user_catalog_name};
# MAGIC USE SCHEMA `bronze`;

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 Exercise 1: Common Table Expressions (CTEs)
# MAGIC
# MAGIC **Goal:** Use layered CTEs to calculate average quantity per order line per supplier, and rank the top 10 suppliers.
# MAGIC
# MAGIC In this activity, we will:
# MAGIC 1. Use one CTE to aggregate total quantity and number of order lines per supplier.
# MAGIC 2. Use another CTE to calculate average quantity per line.
# MAGIC 3. Rank suppliers by this metric in descending order.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Step 1: Create a CTE to aggregate quantity and order line items per supplier from bronze.lineitem
# MAGIC ---- Step 2: Create a second CTE that leverages the output of the first CTE to calculate average quantity per line item 
# MAGIC ---- Step 3: Create a third to rank suppliers by the metric computed in the second CTE
# MAGIC
# MAGIC ---- Replace the code below with your own implementation
# MAGIC
# MAGIC ---- Example structure (fill in logic):
# MAGIC ---- WITH supplier_totals AS (...),
# MAGIC ----      supplier_avg AS (...),
# MAGIC ----      ranked_suppliers AS (...)
# MAGIC ---- SELECT ...
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧾 Exercise 2: Temporary Views
# MAGIC
# MAGIC **Goal:** Creating both session-scoped and global temporary views to stage intermediate logic for reuse.
# MAGIC
# MAGIC In this activity, we will:
# MAGIC 1. Create a `TEMP VIEW` of orders placed in a specific year.
# MAGIC 2. Use that view to calculate average revenue per order.
# MAGIC 3. Create a `GLOBAL TEMP VIEW` for products with the highest total quantity sold.
# MAGIC
# MAGIC > 💡 Remember: A `TEMP VIEW` is scoped to the current session, whereas a `GLOBAL TEMP VIEW` can be shared across sessions via the `global_temp` schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Step 1: Create a TEMP VIEW for orders from a single year (e.g., 1997)
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Step 2: Use the TEMP VIEW to calculate average revenue per order
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Step 3: Create a GLOBAL TEMP VIEW of top-selling products by quantity
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Step 4: Access GLOBAL TEMP VIEW (typically in another session)
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧮 Exercise 3: SQL User-Defined Functions (UDFs)
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

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Step 1: Create UDF is_large_lineitem(qty) → returns TRUE if quantity > 100
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Step 2: Create a second UDF to return 'bulk', 'standard', or 'small' based on quantity
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Step 3: Use your lineitem_size_label(qty) UDF in a GROUP BY query to count each classification
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Exercise 4: EXECUTE IMMEDIATE – Dynamic SQL
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

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Step 1: Declare a variable for a target year
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Step 2: Use EXECUTE IMMEDIATE to construct the full query
# MAGIC ---- 2.1 Dynamically query number of orders and total revenue for that year
# MAGIC ---- 2.2 Add a variable to specify a sort column (e.g., o_totalprice) and ORDER BY it
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎛️ Exercise 5: SQL Widgets – Interactive Inputs for Parameterized Queries
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

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Step 1: Create a TEXT widget for year, 'year_input'
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Step 2: Create a TEXT widget for minimum quantity ,'min_qty_input'
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Step 3: Create a MULTISELECT widget based on an array of CHOICES for product manufacturers, 'mfgr_input'
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Step 4: Query using all widget values
# MAGIC ---- Hint: Use array_contains() and split() for multiselect filtering
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ***

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

# Remove all widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC In this lab, we explored core SQL programming constructs in Databricks using TPCH sample data to build dynamic and maintainable analytical workflows. We practiced using Common Table Expressions for modular query logic, created temporary and global temporary views for reusable results, defined SQL User Defined Functions to encapsulate business rules, executed dynamic SQL with variables at runtime, and applied parameterized SQL for interactive analysis. Mastering these techniques equips you to write cleaner, reusable, and more flexible SQL code for a wide range of real-world data analysis scenarios in Databricks.