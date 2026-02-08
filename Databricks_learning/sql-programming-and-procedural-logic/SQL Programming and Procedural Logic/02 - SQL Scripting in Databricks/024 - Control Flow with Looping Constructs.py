# Databricks notebook source
# MAGIC %md
# MAGIC # Control Flow with Looping Constructs
# MAGIC In this demo, we will learn how to implement control flow in Databricks SQL scripting using the `LOOP` construct. You will practice iterating through part keys in the `lineitem` table, using `LEAVE` to exit the loop and `ITERATE` to skip over records with zero quantity. By the end of this demo, you will understand how to build efficient, conditional logic for selective data processing with SQL loops.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC * Apply the `LOOP` statement to automate repetitive SQL tasks
# MAGIC * Use `LEAVE` and `ITERATE` for flow control within loop constructs
# MAGIC * Demonstrate variable manipulation inside SQL scripts
# MAGIC * Selectively process and output data by iterating through table records

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
# MAGIC ## ⚙️ Exercise: Looping Constructs
# MAGIC
# MAGIC **Goal:** Iterate Through Part Keys and Skip Zero Quantities
# MAGIC
# MAGIC In this activity, we will write a SQL script that:
# MAGIC
# MAGIC 1. Loops through the first five `partkey` values in the `lineitem` table.
# MAGIC 2. Outputs the `partkey` and `quantity` for each.
# MAGIC 3. Skips any row where the quantity is 0 using `ITERATE`.
# MAGIC 4. Uses a loop structure of your choice (`LOOP`, `WHILE`, or `FOR`) along with `LEAVE` and/or `ITERATE` as needed.

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
# MAGIC For this demo, we will use a looping construct to iterate through part keys, skipping records with zero quantity and outputting relevant values step by step:
# MAGIC
# MAGIC * **Step 1: Declare counter and loop max**
# MAGIC     
# MAGIC     * We begin by declaring a counter variable, `i`, initializing it to 1. This counter will track the current part key as we loop through the records.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 1: Declare counter and loop max
# MAGIC         DECLARE i INT DEFAULT 1;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 2: Start labeled loop**
# MAGIC
# MAGIC     * A labeled `LOOP` block named `part_loop` is started, setting the boundary for our iterative logic.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         -- Step 2: Start labeled loop
# MAGIC         part_loop: LOOP
# MAGIC           -- . . .
# MAGIC         ```
# MAGIC
# MAGIC * **Step 3: Define LEAVE condition**
# MAGIC
# MAGIC     * Within the loop, we check if the counter `i` exceeds 5. If it does, we exit the loop using the `LEAVE` statement.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         part_loop: LOOP
# MAGIC           -- Step 3: Define LEAVE condition
# MAGIC           IF i > 5 THEN
# MAGIC             LEAVE part_loop;
# MAGIC           END IF;
# MAGIC           -- Step 4: Skip if l_quantity is zero using ITERATE
# MAGIC           -- . . .
# MAGIC           -- Step 5: Output valid l_partkey and l_quantity
# MAGIC           -- . . .
# MAGIC           -- Step 6: Increment counter
# MAGIC           -- . . .
# MAGIC           -- Step 7: End loop
# MAGIC         END LOOP part_loop;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 4: Skip if l_quantity is zero using ITERATE**
# MAGIC
# MAGIC     * If there are no rows for the current part key where the quantity (`l_quantity`) is greater than 0, we increment `i` and use the `ITERATE` statement to skip to the next iteration, effectively bypassing keys with only zero quantities.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         part_loop: LOOP
# MAGIC           -- Step 3: Define LEAVE condition
# MAGIC           -- . . .
# MAGIC           -- Step 4: Skip if l_quantity is zero using ITERATE
# MAGIC           IF (SELECT COUNT(*) FROM ${DA.user_catalog_name}.bronze.lineitem WHERE l_partkey = i AND l_quantity > 0) = 0 THEN
# MAGIC             SET i = i + 1;
# MAGIC             ITERATE part_loop;
# MAGIC           END IF;
# MAGIC           -- Step 5: Output valid l_partkey and l_quantity
# MAGIC           -- . . .
# MAGIC           -- Step 6: Increment counter
# MAGIC           -- . . .
# MAGIC           -- Step 7: End loop
# MAGIC         END LOOP part_loop;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 5: Output valid l_partkey and l_quantity**
# MAGIC
# MAGIC     * If the part key passes the check, we select and output its `l_partkey` and `l_quantity` values from the `lineitem` table.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         part_loop: LOOP
# MAGIC           -- Step 3: Define LEAVE condition
# MAGIC           -- . . .
# MAGIC           -- Step 4: Skip if l_quantity is zero using ITERATE
# MAGIC           -- . . .
# MAGIC           -- Step 5: Output valid l_partkey and l_quantity
# MAGIC           SELECT l_partkey, l_quantity
# MAGIC           FROM ${DA.user_catalog_name}.bronze.lineitem
# MAGIC           WHERE l_partkey = i;
# MAGIC           -- Step 6: Increment counter
# MAGIC           -- . . .
# MAGIC           -- Step 7: End loop
# MAGIC         END LOOP part_loop;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 6: Increment counter**
# MAGIC
# MAGIC     * After processing the current part key, we increment the counter to move to the next key.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         part_loop: LOOP
# MAGIC           -- Step 3: Define LEAVE condition
# MAGIC           -- . . .
# MAGIC           -- Step 4: Skip if l_quantity is zero using ITERATE
# MAGIC           -- . . .
# MAGIC           -- Step 5: Output valid l_partkey and l_quantity
# MAGIC           -- . . .
# MAGIC           -- Step 6: Increment counter
# MAGIC           SET i = i + 1;
# MAGIC           -- Step 7: End loop
# MAGIC         END LOOP part_loop;
# MAGIC         ```
# MAGIC
# MAGIC * **Step 7: End loop**
# MAGIC
# MAGIC     * Finally, the loop repeats until the exit condition is met and the block is closed.
# MAGIC
# MAGIC     * **Code Snippet:**
# MAGIC         ```sql
# MAGIC         part_loop: LOOP
# MAGIC           -- Step 3: Define LEAVE condition
# MAGIC           -- . . .
# MAGIC           -- Step 4: Skip if l_quantity is zero using ITERATE
# MAGIC           -- . . .
# MAGIC           -- Step 5: Output valid l_partkey and l_quantity
# MAGIC           -- . . .
# MAGIC           -- Step 6: Increment counter
# MAGIC           -- . . .
# MAGIC           -- Step 7: End loop
# MAGIC         END LOOP part_loop;
# MAGIC         ```
# MAGIC
# MAGIC Finally, we will organize all these steps within a `BEGIN...END` block to run the operations together as a single logical unit.

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
# MAGIC   -- Use a loop to iterate through 5 partkey values
# MAGIC   -- Skip any part where quantity is 0
# MAGIC   -- Replace ${DA.user_catalog_name} with your actual catalog name
# MAGIC   BEGIN
# MAGIC     -- Step 1: Declare counter and loop max
# MAGIC     DECLARE i INT DEFAULT 1;
# MAGIC     -- Step 2: Start labeled loop
# MAGIC     part_loop: LOOP
# MAGIC       -- Step 3: Define LEAVE condition
# MAGIC       IF i > 5 THEN
# MAGIC         LEAVE part_loop;
# MAGIC       END IF;
# MAGIC     -- Step 4: Skip if l_quantity is zero using ITERATE
# MAGIC       IF (SELECT COUNT(*) FROM ${DA.user_catalog_name}.bronze.lineitem WHERE l_partkey = i AND l_quantity > 0) = 0 THEN
# MAGIC         SET i = i + 1;
# MAGIC         ITERATE part_loop;
# MAGIC       END IF;
# MAGIC       -- Step 5: Output valid l_partkey and l_quantity
# MAGIC       SELECT l_partkey, l_quantity
# MAGIC       FROM ${DA.user_catalog_name}.bronze.lineitem
# MAGIC       WHERE l_partkey = i;
# MAGIC       -- Step 6: Increment counter
# MAGIC       SET i = i + 1;
# MAGIC     -- Step 7: End loop
# MAGIC     END LOOP part_loop;
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
# MAGIC   ![Query 1 - Output](../Includes/images/control_flow_with_looping.png)
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
# MAGIC In this demo, we used the `LOOP` construct along with `LEAVE` and `ITERATE` in Databricks SQL to control iterative data processing. This allowed us to efficiently step through records, skip those with a quantity of zero, and output relevant results. Mastering these techniques enables you to craft more flexible and automated SQL scripts for complex data workflows.