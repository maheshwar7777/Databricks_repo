# Databricks notebook source
# MAGIC %md
# MAGIC # Conditional Logic
# MAGIC In this demo, we will learn how to use conditional logic in Databricks SQL scripting to dynamically categorize data based on business rules. You will declare variables in a compound statement, use both `IF` and `CASE` logic to classify a supplier's location as Domestic, International, or Unlisted, and see how branching logic enables adaptable query outcomes. By the end of this demo, you will be prepared to implement flexible decision logic within your SQL scripts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC * Apply `IF` and `CASE` statements to branch logic in SQL scripts
# MAGIC * Execute queries with business rule-driven variable assignment
# MAGIC * Demonstrate multiple output paths using nested conditional logic
# MAGIC * Perform dynamic data actions with conditional statements in compound blocks

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
# MAGIC ## 🧮 Exercise: Conditional Logic
# MAGIC
# MAGIC **Goal:**  Use `IF` and `CASE` to Categorize Supplier Location
# MAGIC
# MAGIC In this activity, you'll:
# MAGIC
# MAGIC 1. Declare a variable to hold the country of a supplier (e.g., using supplier key `500`).
# MAGIC 2. Use `IF` or `CASE` logic to classify the supplier as:
# MAGIC    - `'Domestic'` if the country is `'USA'`
# MAGIC    - `'International'` if the country is not `'USA'`
# MAGIC    - `'Unlisted'` if no country value is found
# MAGIC 3. You'll also rewrite your logic using `CASE` for comparison.

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
# MAGIC For this demo, we will develop step-by-step logic to categorize a supplier's location, using both `IF` and `CASE` statements within a compound block:
# MAGIC
# MAGIC * **Step 1: Declare variable to hold country**
# MAGIC     * We will begin by declaring a variable named `supp_country` to store the country associated with a specific supplier.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 1: Declare variable to hold country
# MAGIC         DECLARE supp_country STRING;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 2: Get country of supplier 500**
# MAGIC     * Next, we assign a value to this variable by querying the supplier and nation tables, retrieving the country for the supplier whose key is 500.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 2: Get country of supplier 500
# MAGIC         SET supp_country = (
# MAGIC           SELECT n.n_name
# MAGIC           FROM ${DA.user_catalog_name}.bronze.supplier AS s
# MAGIC           JOIN ${DA.user_catalog_name}.bronze.nation n ON s.s_nationkey = n.n_nationkey
# MAGIC           WHERE s.s_suppkey = 500
# MAGIC         );
# MAGIC         ```
# MAGIC
# MAGIC * **Step 3a: Classification using IF**
# MAGIC
# MAGIC     * We then use an `IF–ELSEIF–ELSE` structure to classify the supplier:  
# MAGIC         - If the country is **not found (null)**, the classification is **'Unlisted'**.  
# MAGIC         - If the country is **'USA'**, the classification is **'Domestic'**.  
# MAGIC         - Otherwise, the classification is **'International'**.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 3a: Classification using IF
# MAGIC         IF supp_country IS NULL THEN
# MAGIC           SELECT 'Unlisted' AS classification;
# MAGIC         ELSEIF supp_country = 'USA' THEN
# MAGIC           SELECT 'Domestic' AS classification;
# MAGIC         ELSE
# MAGIC           SELECT 'International' AS classification;
# MAGIC         END IF;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 3b: Classification using CASE**  
# MAGIC     * Lastly, for comparison, we rewrite the same logic using a SQL CASE statement, again assigning the label 'Unlisted', 'Domestic', or 'International' based on the supplier's country.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 3b: Classification using CASE
# MAGIC         SELECT
# MAGIC           CASE
# MAGIC             WHEN supp_country IS NULL THEN 'Unlisted'
# MAGIC             WHEN supp_country = 'USA' THEN 'Domestic'
# MAGIC             ELSE 'International'
# MAGIC           END AS case_classification;
# MAGIC         ```
# MAGIC
# MAGIC Finally, we'll organize all three steps within a `BEGIN...END` block to run the operations together as a single logical unit.

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
# MAGIC   -- Complete the SQL script using both approaches inside a `BEGIN...END` block.
# MAGIC   -- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC   BEGIN
# MAGIC     -- Step 1: Declare variable to hold country
# MAGIC     DECLARE supp_country STRING;
# MAGIC
# MAGIC     -- Step 2: Get country of supplier 500
# MAGIC     SET supp_country = (
# MAGIC       SELECT n.n_name
# MAGIC       FROM ${DA.user_catalog_name}.bronze.supplier AS s
# MAGIC       JOIN ${DA.user_catalog_name}.bronze.nation n ON s.s_nationkey = n.n_nationkey
# MAGIC       WHERE s.s_suppkey = 500
# MAGIC     );
# MAGIC
# MAGIC     -- Step 3a: Classification using IF
# MAGIC     IF supp_country IS NULL THEN
# MAGIC       SELECT 'Unlisted' AS classification;
# MAGIC     ELSEIF supp_country = 'USA' THEN
# MAGIC       SELECT 'Domestic' AS classification;
# MAGIC     ELSE
# MAGIC       SELECT 'International' AS classification;
# MAGIC     END IF;
# MAGIC
# MAGIC     -- Step 3b: Classification using CASE
# MAGIC     SELECT
# MAGIC       CASE
# MAGIC         WHEN supp_country IS NULL THEN 'Unlisted'
# MAGIC         WHEN supp_country = 'USA' THEN 'Domestic'
# MAGIC         ELSE 'International'
# MAGIC       END AS case_classification;   
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
# MAGIC   ![Query 1 - Output](../Includes/images/conditional_logic.png)
# MAGIC

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
# MAGIC In this demo, we applied conditional logic in Databricks SQL scripts to categorize a supplier's location using both `IF` and `CASE` statements. This approach empowers you to build dynamic, rule-driven workflows that adapt to different scenarios, all while keeping your SQL scripts organized and easy to maintain.