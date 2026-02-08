# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Scripting
# MAGIC In this lab, we will explore advanced SQL scripting techniques in Databricks to create modular, flexible, and resilient workflows. Through a series of practical exercises, you will use compound statements to organize multi-step tasks, define and apply variables for dynamic calculations, implement conditional logic for business rule processing, control data flows with looping constructs, and handle errors with robust exception management. By the end of this lab, you will be able to build well-structured, reusable, and fault-tolerant SQL scripts for complex data analysis in Databricks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC * Apply compound statements to structure multi-step SQL workflows
# MAGIC * Execute variable-driven logic for dynamic SQL scripting
# MAGIC * Demonstrate conditional branching using `IF` and `CASE` statements
# MAGIC * Perform iterative data processing with `LOOP`, `LEAVE`, and `ITERATE`
# MAGIC * Apply exception handling with `SIGNAL` and exit handlers in scripts

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
# MAGIC Throughout this lab, we'll refer to the object `DA`. contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

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
# MAGIC ## 🧱 Exercise 1: Compound Statements
# MAGIC
# MAGIC **Goal:** Use a Compound Statement to Filter and Join
# MAGIC
# MAGIC In this activity, we will create a SQL script that:
# MAGIC
# MAGIC 1. Filters the `orders` table to include only orders with `o_totalprice` less than 10,000
# MAGIC 2. Joins this subset with the `customer` table to retrieve the customer's region (from `c_nationkey` join via `nation` table)
# MAGIC 3. Outputs a summary of the order ID, total price, and the customer's region
# MAGIC
# MAGIC **Note:**
# MAGIC - Use a `BEGIN...END` block to group your logic
# MAGIC - Use `${DA.user_catalog_name}` in place of your catalog name while writing the query in this notebook
# MAGIC - When running in SQL Editor, update `${DA.user_catalog_name}` with your assigned catalog name to ensure the query references your data properly
# MAGIC - **DO NOT** run the code for this exercise in the notebook as it is will only work within the SQL Editor

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Use BEGIN...END block
# MAGIC ---- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC BEGIN
# MAGIC   ---- Step 1: Filter low-value orders
# MAGIC   CREATE OR REPLACE TEMP VIEW low_value_orders AS
# MAGIC   SELECT o_orderkey, o_custkey, o_totalprice
# MAGIC   FROM ${DA.user_catalog_name}.bronze.orders
# MAGIC   WHERE o_totalprice < 10000;
# MAGIC
# MAGIC   ---- Step 2: Join with customer and nation to get region
# MAGIC   CREATE OR REPLACE TEMP VIEW customer_region AS
# MAGIC   SELECT c.c_custkey, c.c_name AS customer_name, n.n_name AS nation_name, n.n_regionkey as regionkey
# MAGIC   FROM ${DA.user_catalog_name}.bronze.customer c
# MAGIC   JOIN ${DA.user_catalog_name}.bronze.nation n ON c.c_nationkey = n.n_nationkey;
# MAGIC
# MAGIC   ---- Step 3: Final join to summarize order data
# MAGIC   SELECT o.o_orderkey, o.o_totalprice, cr.regionkey
# MAGIC   FROM low_value_orders o
# MAGIC   JOIN customer_region cr ON o.o_custkey = cr.c_custkey;
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧾 Exercise 2: SQL Variables
# MAGIC
# MAGIC **Goal:**  Declare and Compare Scalar Values
# MAGIC
# MAGIC In this activity, we will write a script that:
# MAGIC
# MAGIC 1. Declares a variable to hold the median value of `o_totalprice` from the `orders` table
# MAGIC 2. Declares a second variable to hold the **total spending** of customer `250`
# MAGIC 3. Outputs both values and a message comparing whether the customer's spending is above or below the median
# MAGIC
# MAGIC **Note:**
# MAGIC - Use a `BEGIN...END` block to group your logic
# MAGIC - Use `DECLARE` and `SET` for variable creation and assignment
# MAGIC - Use `${DA.user_catalog_name}` in place of your catalog name while writing the query in this notebook
# MAGIC - When running in SQL Editor, update `${DA.user_catalog_name}` with your assigned catalog name to ensure the query references your data properly
# MAGIC - **DO NOT** run the code for this exercise in the notebook as it is will only work within the SQL Editor

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Use BEGIN...END block
# MAGIC ---- Use DECLARE, SET, and SELECT to complete the assignment
# MAGIC ---- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC BEGIN
# MAGIC   ---- Step 1: Declare variables
# MAGIC   DECLARE median_price DOUBLE;
# MAGIC   DECLARE cust_spend DOUBLE;
# MAGIC
# MAGIC   ---- Step 2: Estimate median using percentile approximation
# MAGIC   SET median_price = (
# MAGIC     SELECT PERCENTILE_APPROX(o_totalprice, 0.5) FROM ${DA.user_catalog_name}.bronze.orders
# MAGIC   );
# MAGIC
# MAGIC   ---- Step 3: Get total spending by customer 250
# MAGIC   SET cust_spend = (
# MAGIC     SELECT SUM(o_totalprice) FROM ${DA.user_catalog_name}.bronze.orders WHERE o_custkey = 250
# MAGIC   );
# MAGIC
# MAGIC   ---- Step 4: Output both values
# MAGIC   SELECT median_price AS median_order_total,
# MAGIC         cust_spend AS customer_250_spending;
# MAGIC   
# MAGIC   ---- Step 5 (Optional): derive comparison result inline
# MAGIC   SELECT CASE
# MAGIC     WHEN cust_spend > median_price THEN 'Above Median'
# MAGIC     ELSE 'Below or Equal to Median'
# MAGIC   END AS comparison_result;
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧮 Exercise 3: Conditional Logic
# MAGIC
# MAGIC **Goal:**  Use `IF` and `CASE` to Categorize Supplier Location
# MAGIC
# MAGIC In this activity, you'll:
# MAGIC
# MAGIC 1. Declare a variable to hold the country of a supplier (e.g., using supplier key `500`)
# MAGIC 2. Use `IF` or `CASE` logic to classify the supplier as:
# MAGIC    - `'Domestic'` if the country is `'USA'`
# MAGIC    - `'International'` if the country is not `'USA'`
# MAGIC    - `'Unlisted'` if no country value is found
# MAGIC 3. You'll also rewrite your logic using `CASE` for comparison
# MAGIC
# MAGIC
# MAGIC **Note:**
# MAGIC - Use a `BEGIN...END` block to group your logic
# MAGIC - Use `${DA.user_catalog_name}` in place of your catalog name while writing the query in this notebook
# MAGIC - When running in SQL Editor, update `${DA.user_catalog_name}` with your assigned catalog name to ensure the query references your data properly
# MAGIC - Also rewrite your logic using CASE for comparison
# MAGIC - **DO NOT** run the code for this exercise in the notebook as it is will only work within the SQL Editor

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Use BEGIN...END block
# MAGIC ---- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC BEGIN
# MAGIC   ---- Step 1: Declare variable to hold country
# MAGIC   DECLARE supp_country STRING;
# MAGIC
# MAGIC   ---- Step 2: Get country of supplier 500
# MAGIC   SET supp_country = (
# MAGIC     SELECT n.n_name
# MAGIC     FROM ${DA.user_catalog_name}.bronze.supplier AS s
# MAGIC     JOIN ${DA.user_catalog_name}.bronze.nation n ON s.s_nationkey = n.n_nationkey
# MAGIC     WHERE s.s_suppkey = 500
# MAGIC   );
# MAGIC
# MAGIC   ---- Step 3a: Classification using IF
# MAGIC   IF supp_country IS NULL THEN
# MAGIC     SELECT 'Unlisted' AS classification;
# MAGIC   ELSEIF supp_country = 'USA' THEN
# MAGIC     SELECT 'Domestic' AS classification;
# MAGIC   ELSE
# MAGIC     SELECT 'International' AS classification;
# MAGIC   END IF;
# MAGIC
# MAGIC   ---- Step 3b: Classification using CASE
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN supp_country IS NULL THEN 'Unlisted'
# MAGIC       WHEN supp_country = 'USA' THEN 'Domestic'
# MAGIC       ELSE 'International'
# MAGIC     END AS case_classification;   
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Exercise 4: Looping Constructs
# MAGIC
# MAGIC **Goal:** Iterate Through Part Keys and Skip Zero Quantities
# MAGIC
# MAGIC In this activity, we will write a SQL script that:
# MAGIC
# MAGIC 1. Loops through the first five `partkey` values in the `lineitem` table.
# MAGIC 2. Outputs the `partkey` and `quantity` for each.
# MAGIC 3. Skips any row where the quantity is 0 using `ITERATE`.
# MAGIC 4. Uses a loop structure of your choice (`LOOP`, `WHILE`, or `FOR`) along with `LEAVE` and/or `ITERATE` as needed.
# MAGIC
# MAGIC
# MAGIC **Note:**
# MAGIC - Use a `BEGIN...END` block to group your logic
# MAGIC - Use `${DA.user_catalog_name}` in place of your catalog name while writing the query in this notebook
# MAGIC - When running in SQL Editor, update `${DA.user_catalog_name}` with your assigned catalog name to ensure the query references your data properly
# MAGIC - **DO NOT** run the code for this exercise in the notebook as it is will only work within the SQL Editor

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Use BEGIN...END block
# MAGIC ---- Use a loop to iterate through 5 partkey values
# MAGIC ---- Skip any part where quantity is 0
# MAGIC ---- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC BEGIN
# MAGIC   ---- Step 1: Declare counter and loop max
# MAGIC   DECLARE i INT DEFAULT 1;
# MAGIC
# MAGIC   ---- Step 2: Start labeled loop
# MAGIC   part_loop: LOOP
# MAGIC
# MAGIC     ---- Step 3: Define LEAVE condition
# MAGIC     IF i > 5 THEN
# MAGIC       LEAVE part_loop;
# MAGIC     END IF;
# MAGIC
# MAGIC   ---- Step 4: Skip if l_quantity is zero using ITERATE
# MAGIC     IF (SELECT COUNT(*) FROM ${DA.user_catalog_name}.bronze.lineitem WHERE l_partkey = i AND l_quantity > 0) = 0 THEN
# MAGIC       SET i = i + 1;
# MAGIC       ITERATE part_loop;
# MAGIC     END IF;
# MAGIC
# MAGIC     ---- Step 5: Output valid l_partkey and l_quantity
# MAGIC     SELECT l_partkey, l_quantity
# MAGIC     FROM ${DA.user_catalog_name}.bronze.lineitem
# MAGIC     WHERE l_partkey = i;
# MAGIC
# MAGIC     ---- Step 6: Increment counter
# MAGIC     SET i = i + 1;
# MAGIC
# MAGIC   ---- Step 7: End loop
# MAGIC   END LOOP part_loop;
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎛️ Exercise 5: Error Handling
# MAGIC
# MAGIC **Goal:** Validate Discount Ratio and Raise Error
# MAGIC
# MAGIC In this activity, we will create a SQL script that:
# MAGIC
# MAGIC 1. Calculates the average discount and from the `lineitem` table.
# MAGIC 2. Sets the average discount value to a `discount_avg` variable.
# MAGIC 3. If the average discount exceeds 0.5 (5%), use `SIGNAL` to raise a custom error with a message.
# MAGIC 4. Wrap the entire logic inside a `BEGIN...END` block.
# MAGIC 5. (Optionally) Include an `EXIT HANDLER` for fallback in case of runtime issues (e.g., division by zero).
# MAGIC
# MAGIC
# MAGIC **Notes:**
# MAGIC - Use a `BEGIN...END` block to group your logic
# MAGIC - Write the SQL script using `SIGNAL`, `SQLSTATE`, and (optionally) `DECLARE EXIT HANDLER` constructs.
# MAGIC - Use `${DA.user_catalog_name}` in place of your catalog name while writing the query in this notebook
# MAGIC - When running in SQL Editor, update `${DA.user_catalog_name}` with your assigned catalog name to ensure the query references your data properly
# MAGIC - **DO NOT** run the code for this exercise in the notebook as it is will only work within the SQL Editor
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 1 - Main Query (Tasks 1 to 4):

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Use BEGIN...END block
# MAGIC ---- Use SIGNAL and SQLSTATE to complete the validation logic
# MAGIC ---- Also, use `DECLARE EXIT HANDLER` constructs (optionally)
# MAGIC ---- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC BEGIN
# MAGIC   ---- Step 1: Declare variables for calculation
# MAGIC   DECLARE discount_avg DOUBLE;
# MAGIC
# MAGIC   ---- Step 2: Set values from aggregates
# MAGIC   SET discount_avg = (SELECT AVG(l_discount) FROM ${DA.user_catalog_name}.bronze.lineitem);
# MAGIC
# MAGIC   ---- Step 3: Trigger validation logic
# MAGIC   IF discount_avg > 0.05 THEN
# MAGIC     SIGNAL SQLSTATE '45000'
# MAGIC     SET MESSAGE_TEXT = 'Discount exceeds allowed threshold.';
# MAGIC   END IF;
# MAGIC
# MAGIC   ---- Step 4: Output if no validation triggered
# MAGIC   SELECT 'Validation passed. Discount within acceptable range.' AS status;
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE:**
# MAGIC
# MAGIC * **For the main query:**
# MAGIC     * Having run your script against a threshold of 5% the first time, update the threshold in the code above to 0.04 (4%) and run the script again.

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 2 - Optional Query (Task 5):

# COMMAND ----------

# DBTITLE 1,ANSWER: ERROR HANDLING 2
# MAGIC %sql
# MAGIC ---- (OPTIONAL)
# MAGIC ---- Your Task:
# MAGIC ---- Use BEGIN...END block
# MAGIC ---- Add DECLARE EXIT HANDLER to your use of SIGNAL and SQLSTATE to complete the validation logic
# MAGIC ---- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC BEGIN
# MAGIC   ---- Step 1: Declare variable for calculation
# MAGIC   DECLARE discount_avg DOUBLE;
# MAGIC
# MAGIC   ---- Step 2: Declare handler for any SQL error (e.g., division by zero)
# MAGIC   DECLARE EXIT HANDLER FOR SQLEXCEPTION
# MAGIC   BEGIN
# MAGIC     SELECT 'Error encountered: Fallback handler triggered.' AS status;
# MAGIC   END;
# MAGIC
# MAGIC   ---- Step 3: Set values from aggregates
# MAGIC   SET discount_avg = (SELECT AVG(l_discount) FROM ${DA.user_catalog_name}.bronze.lineitem);
# MAGIC
# MAGIC   ---- Step 4: Trigger validation logic
# MAGIC   IF discount_avg > 0.05 THEN
# MAGIC     SIGNAL SQLSTATE '45000'
# MAGIC     SET MESSAGE_TEXT = 'Discount exceeds allowed threshold.';
# MAGIC   END IF;
# MAGIC
# MAGIC   ---- Step 5: Output if no validation triggered
# MAGIC   SELECT 'Validation passed. Discount within acceptable range.' AS status;
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE:**
# MAGIC
# MAGIC * **For the optional query:**
# MAGIC     * As with the main query, having first run the test against the original threshold value of 5%, change the threshold to 4%, and re-run.
# MAGIC     * **Analyze:** Which error handler takes precedence?

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧾 Exercise 6: Full SQL Scripting Workflow
# MAGIC
# MAGIC **Goal:** Build a complete SQL script that performs this validation scenario from end to end
# MAGIC
# MAGIC In this activity, we will write a SQL script that:
# MAGIC
# MAGIC 1. Declare variables for:
# MAGIC    - A target ship date (e.g., `'1997-01-01'`)
# MAGIC    - A threshold value (e.g., 5000 line items)
# MAGIC 2. Count the number of line items shipped on the target date.
# MAGIC 3. Use `IF` logic to compare the count to the threshold.
# MAGIC 4. Use `SIGNAL` to raise an error if the threshold is not met.
# MAGIC 5. Use an `EXIT HANDLER` to allow fallback messaging in case of runtime issues.
# MAGIC
# MAGIC
# MAGIC **Note:**
# MAGIC - Use a `BEGIN...END` block to group your logic
# MAGIC - Use the `DECLARE`, `SET`, `IF`, `SIGNAL`, and `HANDLER` as needed
# MAGIC - Use `${DA.user_catalog_name}` in place of your catalog name while writing the query in this notebook
# MAGIC - When running in SQL Editor, update `${DA.user_catalog_name}` with your assigned catalog name to ensure the query references your data properly
# MAGIC - **DO NOT** run the code for this exercise in the notebook as it is will only work within the SQL Editor

# COMMAND ----------

# DBTITLE 1,ANSWER: Capstone
# MAGIC %sql
# MAGIC ---- Your Task:
# MAGIC ---- Build your full validation workflow below
# MAGIC ---- Use BEGIN...END block
# MAGIC ---- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC BEGIN
# MAGIC   ---- Step 1: Declare variables
# MAGIC   DECLARE target_date DATE DEFAULT DATE '1997-01-01';
# MAGIC   DECLARE lineitem_threshold INT DEFAULT 5000;
# MAGIC   DECLARE lineitem_count INT;
# MAGIC
# MAGIC   ---- Step 2: Handler for fallback messaging
# MAGIC   DECLARE EXIT HANDLER FOR SQLEXCEPTION
# MAGIC   BEGIN
# MAGIC     SELECT 'Runtime error occurred. Executing fallback handler.' AS handler_message;
# MAGIC   END;
# MAGIC
# MAGIC   ---- Step 3: Count lineitems shipped on the target date
# MAGIC   SET lineitem_count = (
# MAGIC     SELECT COUNT(*) FROM ${DA.user_catalog_name}.bronze.lineitem l
# MAGIC     WHERE l.l_shipdate = target_date
# MAGIC   );
# MAGIC
# MAGIC   ---- Step 4: Trigger signal if below threshold
# MAGIC   IF lineitem_count < lineitem_threshold THEN
# MAGIC     SIGNAL SQLSTATE '45000'
# MAGIC     SET MESSAGE_TEXT = 'Lineitem volume below threshold. Aborting downstream steps.';
# MAGIC   END IF;
# MAGIC
# MAGIC   ---- Step 5: Output if validation passes
# MAGIC   SELECT 'Validation passed. Lineitem volume meets threshold.' AS validation_status;
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ---

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
# MAGIC In this lab, we explored advanced SQL scripting techniques in Databricks to create structured, flexible, and resilient workflows. We practiced using compound statements to organize multi-step SQL tasks, applied variables for dynamic calculations, implemented conditional logic with `IF` and `CASE` statements, controlled execution flow with looping constructs such as `LOOP`, `LEAVE`, and `ITERATE`, and managed errors using `SIGNAL` and exit handlers. Mastering these techniques equips you to build modular, reusable, and fault-tolerant SQL scripts that can handle complex data processing and analysis scenarios in Databricks.