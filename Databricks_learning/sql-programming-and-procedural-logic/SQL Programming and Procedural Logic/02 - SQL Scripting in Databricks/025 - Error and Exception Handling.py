# Databricks notebook source
# MAGIC %md
# MAGIC # Error and Exception Handling
# MAGIC In this demo, we will learn how to implement error and exception handling in Databricks SQL scripting. Using `SIGNAL` statements and exit handlers, you will see how to manage and customize responses to errors or validation failures—such as when calculated values exceed set thresholds. By the end of this demo, you will know how to build robust SQL scripts that halt, recover, or provide clear feedback in the face of unexpected issues.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC * Apply exit handlers to manage exceptions in SQL scripts
# MAGIC * Execute controlled error responses with SIGNAL and SQLSTATE
# MAGIC * Demonstrate custom messaging for failed validation or runtime errors
# MAGIC * Implement validation checks that halt or signal errors in your SQL scripts

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
# MAGIC ## 🎛️ Exercise: Error Handling
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
# MAGIC **Main Task Details \(Tasks 1 to 4\):**
# MAGIC
# MAGIC For the main tasks \(i.e., tasks 1 to 4\) of this demo, we will write a compound SQL script to validate discount values, and signal an error if the average exceeds the allowed threshold:
# MAGIC
# MAGIC * **Step 1: Declare variables for calculation**
# MAGIC
# MAGIC     * We will start by declaring a variable, `discount_avg`, to hold the computed average discount from the table.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 1: Declare variables for calculation
# MAGIC         DECLARE discount_avg DOUBLE;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 2: Set values from aggregates**
# MAGIC
# MAGIC     * We will now calculate the average discount by aggregating values from the `lineitem` table and assign the result to `discount_avg`.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 2: Set values from aggregates
# MAGIC         SET discount_avg = (SELECT AVG(l_discount) FROM ${DA.user_catalog_name}.bronze.lineitem);
# MAGIC         ```
# MAGIC
# MAGIC * **Step 3: Trigger validation logic**
# MAGIC
# MAGIC     * If the computed average discount is greater than the specified threshold (for example, 0.05 for 5%), immediately raise a custom error using the `SIGNAL` statement with a specific SQL state and error message.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 3: Trigger validation logic
# MAGIC         IF discount_avg > 0.05 THEN
# MAGIC           SIGNAL SQLSTATE '45000'
# MAGIC           SET MESSAGE_TEXT = 'Discount exceeds allowed threshold.';
# MAGIC         END IF;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 4: Output if no validation triggered**
# MAGIC
# MAGIC     * If the average discount does not exceed the threshold, output a message indicating validation has passed.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 4: Output if no validation triggered
# MAGIC         SELECT 'Validation passed. Discount within acceptable range.' AS status;
# MAGIC         ```
# MAGIC
# MAGIC Finally, we will organize all these steps within a `BEGIN...END` block to run the operations together as a single logical unit.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC **\(Optional\) Task Details \(Task 5 only\):**
# MAGIC
# MAGIC Next, as an optional exercise, we will enhance our error handling by combining custom error signaling \(`SIGNAL`\) with an exit handler \(`EXIT HANDLER`\) in a single SQL script block to observe how they interact during execution.
# MAGIC
# MAGIC * **Step 1: Declare variable for calculation**
# MAGIC
# MAGIC     * Declare `discount_avg` as a variable to hold the average discount.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 1: Declare variable for calculation
# MAGIC         DECLARE discount_avg DOUBLE;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 2: Declare handler for any SQL error (e.g., division by zero)**
# MAGIC
# MAGIC     * Define an `EXIT HANDLER FOR SQLEXCEPTION`, specifying logic to run if a runtime SQL error occurs—such as outputting a fallback message.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 2: Declare handler for any SQL error (e.g., division by zero)
# MAGIC         DECLARE EXIT HANDLER FOR SQLEXCEPTION
# MAGIC         BEGIN
# MAGIC           SELECT 'Error encountered: Fallback handler triggered.' AS status;
# MAGIC         END;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 3: Set values from aggregates**
# MAGIC
# MAGIC     * Assign the calculated average discount from the `lineitem` table to `discount_avg`.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 3: Set values from aggregates
# MAGIC         SET discount_avg = (SELECT AVG(l_discount) FROM ${DA.user_catalog_name}.bronze.lineitem);
# MAGIC         ```
# MAGIC
# MAGIC * **Step 4: Trigger validation logic**
# MAGIC
# MAGIC     * Check if `discount_avg` exceeds the set threshold. If it does, use `SIGNAL` and `SQLSTATE` to manually trigger an error with a custom message.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 4: Trigger validation logic
# MAGIC         IF discount_avg > 0.05 THEN
# MAGIC           SIGNAL SQLSTATE '45000'
# MAGIC           SET MESSAGE_TEXT = 'Discount exceeds allowed threshold.';
# MAGIC         END IF;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 5: Output if no validation triggered**
# MAGIC
# MAGIC     * If validation passes, return a success message stating the discount is within the acceptable range.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 5: Output if no validation triggered
# MAGIC         SELECT 'Validation passed. Discount within acceptable range.' AS status;
# MAGIC         ```
# MAGIC
# MAGIC Finally, we will organize all three steps within a `BEGIN...END` block to run the operations together as a single logical unit.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC #### Implimentation Instructions
# MAGIC
# MAGIC ##### 1. Open the SQL Editor 📝
# MAGIC - In the left-hand Databricks sidebar, right-click on **SQL Editor** and select the **"Open Link in New Tab"** option.
# MAGIC
# MAGIC ##### 2. Paste and Update Your SQL Code for the Main Tasks ⌨️
# MAGIC - In the new, blank Query tab, paste the following SQL code for the **main** tasks *(make sure to update the catalog as instructed)*:
# MAGIC
# MAGIC   ```sql
# MAGIC   -- Use SIGNAL and SQLSTATE to complete the validation logic
# MAGIC   -- Also, use `DECLARE EXIT HANDLER` constructs (optionally)
# MAGIC   -- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC   BEGIN
# MAGIC     -- Step 1: Declare variables for calculation
# MAGIC     DECLARE discount_avg DOUBLE;
# MAGIC
# MAGIC     -- Step 2: Set values from aggregates
# MAGIC     SET discount_avg = (SELECT AVG(l_discount) FROM ${DA.user_catalog_name}.bronze.lineitem);
# MAGIC
# MAGIC     -- Step 3: Trigger validation logic
# MAGIC     IF discount_avg > 0.05 THEN
# MAGIC       SIGNAL SQLSTATE '45000'
# MAGIC       SET MESSAGE_TEXT = 'Discount exceeds allowed threshold.';
# MAGIC     END IF;
# MAGIC
# MAGIC     -- Step 4: Output if no validation triggered
# MAGIC     SELECT 'Validation passed. Discount within acceptable range.' AS status;
# MAGIC   END;
# MAGIC   ```
# MAGIC
# MAGIC ##### 3. Paste and Update Your SQL Code for the Optional Task ⌨️
# MAGIC - Click on the **plus icon (`+`)** next to the existing query tab and select the **Create new query** option.
# MAGIC   ![New Query Option](../Includes/images/new_query_tab_1.png)<br/><br/>
# MAGIC - In the new, blank Query tab, paste the following SQL code for the **optional** task *(make sure to update the catalog as instructed)*:
# MAGIC
# MAGIC   ```sql
# MAGIC   -- (OPTIONAL)
# MAGIC   -- Add DECLARE EXIT HANDLER to your use of SIGNAL and SQLSTATE to complete the validation logic
# MAGIC   -- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC   BEGIN
# MAGIC     -- Step 1: Declare variable for calculation
# MAGIC     DECLARE discount_avg DOUBLE;
# MAGIC
# MAGIC     -- Step 2: Declare handler for any SQL error (e.g., division by zero)
# MAGIC     DECLARE EXIT HANDLER FOR SQLEXCEPTION
# MAGIC     BEGIN
# MAGIC       SELECT 'Error encountered: Fallback handler triggered.' AS status;
# MAGIC     END;
# MAGIC
# MAGIC     -- Step 3: Set values from aggregates
# MAGIC     SET discount_avg = (SELECT AVG(l_discount) FROM ${DA.user_catalog_name}.bronze.lineitem);
# MAGIC
# MAGIC     -- Step 4: Trigger validation logic
# MAGIC     IF discount_avg > 0.05 THEN
# MAGIC       SIGNAL SQLSTATE '45000'
# MAGIC       SET MESSAGE_TEXT = 'Discount exceeds allowed threshold.';
# MAGIC     END IF;
# MAGIC
# MAGIC     -- Step 5: Output if no validation triggered
# MAGIC     SELECT 'Validation passed. Discount within acceptable range.' AS status;
# MAGIC   END;
# MAGIC   ```
# MAGIC
# MAGIC ##### 4. Replace the Catalog Name 🔗
# MAGIC
# MAGIC - Update `${DA.user_catalog_name}` with your assigned catalog name in both the tabs to ensure the query references your data properly.
# MAGIC
# MAGIC ##### 5. Remove the Row Limit
# MAGIC - Before running and saving, ensure there are no row result limits:
# MAGIC   - Click the dropdown next to the **Run** button in the query editor.
# MAGIC   - Uncheck the row limit (default is 1,000 rows) for complete results in automated jobs.
# MAGIC
# MAGIC ##### 6. Run Query ▶️
# MAGIC
# MAGIC - Switch to the other tab has the **main task** query.
# MAGIC - Then, click the **Run** button to execute your query in the tab
# MAGIC - **(Optional)** Next, switch to the other tab which has the **optional task** query
# MAGIC - **(Optional)** Again, click the **Run** button to execute your query in the tab
# MAGIC
# MAGIC <br/><br/>
# MAGIC #### Final Output Preview - **`Main Tasks`** Query 🖥️
# MAGIC
# MAGIC Once you have lifted the row limit and run your query, your output will look like this:
# MAGIC   ![Query 1 - Main Tasks Query Output](../Includes/images/main_query_output.png)
# MAGIC
# MAGIC <br/><br/>
# MAGIC #### Final Output Preview - **`Optional Task`** Query 🖥️
# MAGIC
# MAGIC Once you have lifted the row limit and run your query, your output will look like this:
# MAGIC   ![Query 1 - Optional Task Query Output](../Includes/images/optional_query_output.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE:**
# MAGIC
# MAGIC * **For the main query:**
# MAGIC     * Having run your script against a threshold of 5% the first time, update the threshold in the code above to 0.04 (4%) and run the script again.
# MAGIC
# MAGIC * **For the optional query:**
# MAGIC     * As with the main query, having first run the test against the original threshold value of 5%, change the threshold to 4%, and re-run.
# MAGIC     * **Analyze:** Which error handler takes precedence?

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
# MAGIC In this demo, we used error and exception handling techniques in Databricks SQL, leveraging exit handlers and custom `SIGNAL` statements to validate data integrity and manage failures. With these approaches, you can detect runtime issues, return meaningful messages, and ensure your SQL workflows respond predictably and safely when problems arise.