# Databricks notebook source
# MAGIC %md
# MAGIC # Common Table Expressions
# MAGIC In this demo, we will learn how to use Common Table Expressions (CTEs) to organize and simplify complex SQL queries within Databricks. Using  sample data, we’ll walk through a sequence of steps to aggregate total quantities and order line counts for each supplier, calculate average quantity per order line, and rank suppliers based on this metric. By the end of this demo, you will see how CTEs allow you to build multi-stage analytical queries in a clear, modular, and maintainable way.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC * Apply Common Table Expressions (CTEs) to organize SQL queries in Databricks
# MAGIC * Execute multi-step SQL queries using CTEs for data aggregation and ranking
# MAGIC * Perform customer order analysis using sample datasets with modular query logic
# MAGIC * Demonstrate normalization techniques to compare customer performance across regions

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

# MAGIC %restart_python

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
# MAGIC ## Using Unity Catalog's Three-Level Namespace
# MAGIC
# MAGIC Anyone with SQL experience will likely be familiar with the traditional two-level namespace to address tables or views within a schema schema as shown in this example query:
# MAGIC
# MAGIC     SELECT * FROM schema.table;
# MAGIC
# MAGIC Unity Catalog introduces the concept of a **catalog** into the hierarchy. As a container for schemas, the catalog provides a new way for organizations to segregate their data. This can be handy in many use cases. For example:
# MAGIC
# MAGIC * Separating data relating to business units within your organization (sales, marketing, human resources, etc)
# MAGIC * Satisfying SDLC requirements (dev, staging, prod, etc)
# MAGIC * Establishing sandboxes containing temporary datasets for internal use
# MAGIC
# MAGIC There can be as many catalogs as you like, which in turn can contain as many schemas as you like. To deal with this additional level, complete table/view references in Unity Catalog use a three-level namespace. This three-level namespace query exemplifies this:
# MAGIC
# MAGIC     SELECT * FROM catalog.schema.table;
# MAGIC     
# MAGIC SQL developers will probably also be familiar with the **`USE`** statement to select a default schema, thereby shortening queries by not having to specify it all the time. To extend this convenience while dealing with the extra level in the namespace, Unity Catalog augments the language with two additional statements, as shown in this example:
# MAGIC
# MAGIC     USE CATALOG mycatalog;  -- To set my own catalog as the default catalog
# MAGIC     USE SCHEMA myschema;  -- To set my own schema created inside my own catalog as the default schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 Exercise: Common Table Expressions (CTEs)
# MAGIC
# MAGIC **Goal:** Use layered CTEs to calculate average quantity per order line per supplier, and rank the top 10 suppliers.
# MAGIC
# MAGIC In this activity, we will:
# MAGIC 1. Use one CTE to aggregate total quantity and number of order lines per supplier.
# MAGIC 2. Use another CTE to calculate average quantity per line.
# MAGIC 3. Rank suppliers by this metric in descending order.

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
# MAGIC For this demo, we will create not just one, but three Common Table Expressions (CTEs):
# MAGIC
# MAGIC * **Step 1:** Supplier Totals
# MAGIC     * Our first CTE will be called `supplier_totals`. Here, we aggregate the total quantity and the number of order lines for each supplier by grouping the `lineitem` table on the supplier key (`l_suppkey`). This gives us, for every supplier, both the total amount they've supplied and how many order lines they are associated with.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         ---- Step 1: Aggregate total quantity and line count per supplier
# MAGIC         WITH supplier_totals AS (
# MAGIC           SELECT
# MAGIC             l_suppkey,
# MAGIC             SUM(l_quantity) AS total_qty,
# MAGIC             COUNT(*) AS num_lines
# MAGIC           FROM ${DA.user_catalog_name}.bronze.lineitem
# MAGIC           GROUP BY l_suppkey
# MAGIC         ),
# MAGIC         ```
# MAGIC
# MAGIC * **Step 2:** Supplier Average
# MAGIC     * Next, we define a second CTE called `supplier_avg`. This CTE calculates the average quantity per order line for each supplier by dividing the total quantity by the number of order lines, using the values computed in the previous CTE. This metric gives insight into the typical size of each supplier's order lines.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         ---- Step 2: Calculate average quantity per order line
# MAGIC         supplier_avg AS (
# MAGIC         SELECT
# MAGIC             l_suppkey,
# MAGIC             total_qty / num_lines AS avg_qty_per_line
# MAGIC         FROM supplier_totals
# MAGIC         ),
# MAGIC         ```
# MAGIC
# MAGIC * **Step 3:** Ranked Suppliers
# MAGIC     * Finally, we construct a third CTE named `ranked_suppliers`. This CTE joins the calculated averages with the supplier names, and then uses a ranking function to rank suppliers in descending order based on their average quantity per order line. This makes it easy to identify the top suppliers according to this metric.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         ---- Step 3: Rank suppliers by average quantity per line
# MAGIC         ranked_suppliers AS (
# MAGIC         SELECT
# MAGIC             sa.l_suppkey,
# MAGIC             s.s_name,
# MAGIC             sa.avg_qty_per_line,
# MAGIC             RANK() OVER (ORDER BY sa.avg_qty_per_line DESC) AS rank
# MAGIC         FROM supplier_avg sa
# MAGIC         JOIN ${DA.user_catalog_name}.bronze.supplier s ON sa.l_suppkey = s.s_suppkey
# MAGIC         )
# MAGIC         ```
# MAGIC
# MAGIC We will encapsulate all three CTEs within a single SQL block to identify and rank the top 10 suppliers by average quantity per order line.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the below cell to perform this operation in a single SQL query:

# COMMAND ----------

# DBTITLE 1,ANSWER: CTEs
# MAGIC %sql
# MAGIC ---- Step 1: Aggregate total quantity and line count per supplier
# MAGIC WITH supplier_totals AS (
# MAGIC   SELECT
# MAGIC     l_suppkey,
# MAGIC     SUM(l_quantity) AS total_qty,
# MAGIC     COUNT(*) AS num_lines
# MAGIC   FROM ${DA.user_catalog_name}.bronze.lineitem
# MAGIC   GROUP BY l_suppkey
# MAGIC ),
# MAGIC ---- Step 2: Calculate average quantity per order line
# MAGIC supplier_avg AS (
# MAGIC   SELECT
# MAGIC     l_suppkey,
# MAGIC     total_qty / num_lines AS avg_qty_per_line
# MAGIC   FROM supplier_totals
# MAGIC ),
# MAGIC ---- Step 3: Rank suppliers by average quantity per line
# MAGIC ranked_suppliers AS (
# MAGIC   SELECT
# MAGIC     sa.l_suppkey,
# MAGIC     s.s_name,
# MAGIC     sa.avg_qty_per_line,
# MAGIC     RANK() OVER (ORDER BY sa.avg_qty_per_line DESC) AS rank
# MAGIC   FROM supplier_avg sa
# MAGIC   JOIN ${DA.user_catalog_name}.bronze.supplier s ON sa.l_suppkey = s.s_suppkey
# MAGIC )
# MAGIC ---- Final result: top 10 suppliers
# MAGIC SELECT *
# MAGIC FROM ranked_suppliers
# MAGIC WHERE rank <= 10;

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
# MAGIC In this demo, we explored how to use Common Table Expressions in Databricks SQL to analyze supplier order line data. By building layered CTEs, we first aggregated total quantities and order line counts per supplier, then calculated each supplier’s average quantity per order line. Finally, we ranked suppliers according to this metric, making it easy to identify top performers. This process highlights how CTEs enable you to structure multi-step analytics tasks in clear, manageable stages, greatly improving the readability and flexibility of complex SQL queries in Databricks.