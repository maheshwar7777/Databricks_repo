# Databricks notebook source
# MAGIC %md
# MAGIC # Temporary Views
# MAGIC
# MAGIC In this demo, we will learn how to use temporary views and global temporary views in Databricks SQL to manage and reuse query results during data analysis. Through a hands-on sequence of steps, you’ll create in-memory temp views within a session, utilize global temp views across sessions, and combine these views seamlessly with TPCH sample data for analysis. By the end of this demo, you will know when and how to apply temporary views to streamline SQL workflows in Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC * Apply temporary views to store intermediate query results in Databricks SQL
# MAGIC * Execute queries using both session-scoped and global temporary views
# MAGIC * Demonstrate how to join temp views with existing tables for deeper analysis
# MAGIC * Perform collaborative analysis by leveraging global temp views across sessions

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
# MAGIC * Access to **TPCH sample datasets** or equivalent data within your workspace  
# MAGIC * **CREATE CATALOG** and **USE CATALOG** privileges to create and manage catalogs  
# MAGIC * **CREATE SCHEMA** and **USE SCHEMA** privileges to create and manage schemas  
# MAGIC * **SELECT** privileges on relevant TPCH tables (such as lineitem and orders)

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
# MAGIC ## Temporary Views in Databricks SQL
# MAGIC
# MAGIC Let's begin by clarifying what a temporary view is. Temporary views are named query results that exist only within your current session. Unlike common table expressions, which are written as part of a single query, temporary views let you define reusable datasets that persist throughout your active notebook or session. This means you can reference the same view across multiple cells, streamlining your workflow.
# MAGIC
# MAGIC It's important to remember that the natural scope of a temporary view is the session. As soon as your session ends, that temporary view is discarded from memory. Temporary views are ideal for tasks where you need to reference intermediate query results several times within the same session, without persisting them to storage. **You cannot reference a temporary view using your three-level namespace** since they are **not stored in the underlying schema or catalog**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Exercise: Temporary Views
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
# MAGIC Let us start by creating a temporary view of all orders placed in the year 1997. After defining this temporary view, it exists in memory and can be queried throughout our active session.
# MAGIC
# MAGIC Run the below cell to perform this operation:

# COMMAND ----------

# DBTITLE 1,ANSWER: TEMP VIEW
# MAGIC %sql
# MAGIC ---- Step 1: Create a TEMP VIEW for orders from a single year (e.g., 1997)
# MAGIC CREATE OR REPLACE TEMP VIEW orders_1997 AS
# MAGIC SELECT *
# MAGIC FROM ${DA.user_catalog_name}.bronze.orders
# MAGIC WHERE year(o_orderdate) = 1997;

# COMMAND ----------

# MAGIC %md
# MAGIC Run the below cell to anayze the contents of this view:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_1997;

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2:**
# MAGIC
# MAGIC Next, we will calculate the average total revenue per order from the 1997 orders temporary view.
# MAGIC
# MAGIC Run the below cell to perform this operation:

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Step 2: Use the TEMP VIEW to calculate average revenue per order
# MAGIC SELECT
# MAGIC   AVG(o_totalprice) AS avg_revenue_1997
# MAGIC FROM orders_1997;

# COMMAND ----------

# MAGIC %md
# MAGIC While temporary views are useful within a single session, there are times when you want to share temporary query results across multiple user sessions, such as for collaborative projects. In these cases, global temporary views come into play.
# MAGIC
# MAGIC A global temporary view, as the name suggests, can be accessed by different sessions and even by different users, as long as the workspace cluster is running. This is achieved by creating the view within a special schema called `global_temp`. The process is similar: you use the `CREATE GLOBAL TEMP VIEW` statement, and the resulting view remains available as long as the cluster is active.

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3:**
# MAGIC
# MAGIC Next, we will create a global temporary view containing the top 10 products ranked by total quantity sold.
# MAGIC
# MAGIC Run the below cell to perform this operation:

# COMMAND ----------

# DBTITLE 1,ANSWER: GLOBAL TEMP VIEW
# MAGIC %sql
# MAGIC ---- Step 3: Create a GLOBAL TEMP VIEW of top-selling products by quantity
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW top_selling_products AS
# MAGIC SELECT
# MAGIC   l_partkey,
# MAGIC   SUM(l_quantity) AS total_quantity
# MAGIC FROM ${DA.user_catalog_name}.bronze.lineitem
# MAGIC GROUP BY l_partkey
# MAGIC ORDER BY total_quantity DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4:**
# MAGIC
# MAGIC Finally, we will query the global temporary view to view the top-selling products by quantity.
# MAGIC
# MAGIC Run the below cell to perform this operation:

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Step 4: Access GLOBAL TEMP VIEW (typically in another session)
# MAGIC SELECT * FROM global_temp.top_selling_products;

# COMMAND ----------

# MAGIC %md
# MAGIC It's crucial to reference the full path `global_temp.top_selling_products` when querying this type of view.
# MAGIC
# MAGIC We can also join our temporary view to other tables for enhanced analysis. This enables your temporary view to be integrated with your broader queries within the session.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selecting the Right View
# MAGIC
# MAGIC * When deciding which view to use, remember:
# MAGIC     * Regular temporary views are scoped to your session—ideal for one-off or interactive analysis.
# MAGIC
# MAGIC
# MAGIC     * Global temporary views are scoped to the workspace cluster and can be accessed by multiple users or notebooks while the cluster is running.
# MAGIC
# MAGIC * Finally, a key consideration:
# MAGIC <br/><br/>
# MAGIC
# MAGIC     * The **global temporary views** are not supported on serverless SQL warehouses. You must use a standard Databricks cluster to create and persist a global temp view. If your workflow requires sharing temporary results across sessions, ensure you are working on a full cluster rather than serverless infrastructure.
# MAGIC
# MAGIC With this understanding, you can choose the right type of temporary view for your Databricks workloads, making your queries both modular and collaborative.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up
# MAGIC Run the below cell to delete the temporary view created for your current session:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the view orders_1997 from your current session.
# MAGIC DROP VIEW IF EXISTS orders_1997;

# COMMAND ----------

# MAGIC %md
# MAGIC Run the below cell to delete the global temporary view created within the `global_temp` schema:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the view "top_selling_products" from the "global_temp" schema.
# MAGIC DROP VIEW IF EXISTS global_temp.top_selling_products;

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
# MAGIC In this demo, we applied Databricks SQL to create and use both temporary and global temporary views for flexible data analysis. We saw how temp views provide in-session modularity while global temp views enable reuse across user sessions, and how both can be integrated with existing tables for targeted queries. By mastering these techniques, you can make your analytic workflows more dynamic, collaborative, and efficient within Databricks.