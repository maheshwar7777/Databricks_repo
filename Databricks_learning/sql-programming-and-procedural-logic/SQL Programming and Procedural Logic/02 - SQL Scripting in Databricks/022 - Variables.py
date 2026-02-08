# Databricks notebook source
# MAGIC %md
# MAGIC # Variables
# MAGIC In this demo, we will learn how to define and use variables in Databricks SQL scripting to support dynamic and flexible analytics. You will see how to declare variables, assign them values from aggregate queries, and use them within a compound statement. By following these steps, you will gain the ability to make your SQL code more modular and maintainable when performing calculations and comparisons.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC * Apply variables to store and reuse scalar values in SQL scripts
# MAGIC * Execute queries that incorporate dynamically assigned variables
# MAGIC * Demonstrate conditional logic using SQL variables in compound statements
# MAGIC * Perform calculations and comparisons using variables within modular workflows

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
# MAGIC ## 🧾 Exercise: SQL Variables
# MAGIC
# MAGIC **Goal:**  Declare and Compare Scalar Values
# MAGIC
# MAGIC In this activity, we will write a script that:
# MAGIC
# MAGIC 1. Declares a variable to hold the median value of `o_totalprice` from the `orders` table.
# MAGIC 2. Declares a second variable to hold the **total spending** of customer `250`.
# MAGIC 3. Outputs both values and a message comparing whether the customer's spending is above or below the median.

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
# MAGIC For this demo, we will declare two scalar variables and perform a step-by-step comparison of customer spending against the median order value:
# MAGIC
# MAGIC * **Step 1: Declare variables**  
# MAGIC     * First, we declare two variables. One will store the median order total price across all orders, while the other will hold the total spending amount for customer 250.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         ---- Step 1: Declare variables
# MAGIC         DECLARE median_price DOUBLE;
# MAGIC         DECLARE cust_spend DOUBLE;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 2: Estimate median using percentile approximation**
# MAGIC
# MAGIC     * Next, we calculate the median value of `o_totalprice` from the orders table using the `PERCENTILE_APPROX` function, and assign this value to the `median_price` variable.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         ---- Step 2: Estimate median using percentile approximation
# MAGIC         SET median_price = (
# MAGIC           SELECT PERCENTILE_APPROX(o_totalprice, 0.5) FROM ${DA.user_catalog_name}.bronze.orders
# MAGIC         );
# MAGIC         ```
# MAGIC
# MAGIC * **Step 3: Get total spending by customer 250**
# MAGIC
# MAGIC     * Here, we sum the `o_totalprice` for all orders placed by customer 250 and assign that amount to the `cust_spend` variable.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         ---- Step 3: Get total spending by customer 250
# MAGIC         SET cust_spend = (
# MAGIC           SELECT SUM(o_totalprice) FROM ${DA.user_catalog_name}.bronze.orders WHERE o_custkey = 250
# MAGIC         );
# MAGIC         ```
# MAGIC
# MAGIC * **Step 4: Output both values**
# MAGIC
# MAGIC     * Finally, we will output both the median order value and the total spending of customer 250 for comparison.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         ---- Step 4: Output both values
# MAGIC         SELECT median_price AS median_order_total,
# MAGIC                cust_spend AS customer_250_spending;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 5 (Optional): derive comparison result inline**
# MAGIC
# MAGIC     * Optionally, we can use a `CASE` statement to display whether customer 250's total spending is above, below, or equal to the median order value. But since it is not mandatory, we will skip this step in this demo.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         ---- Step 5 (Optional): derive comparison result inline
# MAGIC         SELECT CASE 
# MAGIC           WHEN cust_spend > median_price THEN 'Above Median'
# MAGIC           ELSE 'Below or Equal to Median'
# MAGIC         END AS comparison_result;
# MAGIC         ```
# MAGIC
# MAGIC Finally, we will organize all the steps within a `BEGIN...END` block to run the operations together as a single logical unit.

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
# MAGIC   -- Use DECLARE, SET, and SELECT to complete the assignment
# MAGIC   -- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC   BEGIN
# MAGIC     -- Step 1: Declare variables
# MAGIC     DECLARE median_price DOUBLE;
# MAGIC     DECLARE cust_spend DOUBLE;
# MAGIC
# MAGIC     -- Step 2: Estimate median using percentile approximation
# MAGIC     SET median_price = (
# MAGIC       SELECT PERCENTILE_APPROX(o_totalprice, 0.5) FROM ${DA.user_catalog_name}.bronze.orders
# MAGIC     );
# MAGIC
# MAGIC     -- Step 3: Get total spending by customer 250
# MAGIC     SET cust_spend = (
# MAGIC       SELECT SUM(o_totalprice) FROM ${DA.user_catalog_name}.bronze.orders WHERE o_custkey = 250
# MAGIC     );
# MAGIC
# MAGIC     -- Step 4: Output both values
# MAGIC     SELECT median_price AS median_order_total,
# MAGIC           cust_spend AS customer_250_spending;
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
# MAGIC   ![Query 1 - Output](../Includes/images/comparing_customer_spending.png)
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
# MAGIC In this demo, we used variables in Databricks SQL scripting to calculate and compare customer spending against the median order value. By encapsulating logic within a compound statement and using variables for calculation and comparison, we made our queries easier to read, maintain, and expand. These techniques help streamline data analysis and build more flexible SQL scripts.