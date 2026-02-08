# Databricks notebook source
# MAGIC %md
# MAGIC ### Recursive Common Table Expressions (CTEs)
# MAGIC
# MAGIC In this demo, we will learn how to use Recursive Common Table Expressions (CTEs) to traverse hierarchical data such as organizational structures or bill-of-materials in Databricks SQL. Using a sample employee hierarchy, we’ll walk through steps to build a query that recursively expands the reporting chain for a given manager.
# MAGIC
# MAGIC By the end of this demo, you will see how recursive CTEs allow you to elegantly handle hierarchical or tree-structured data.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Apply recursive CTEs to query hierarchical data in Databricks
# MAGIC - Execute multi-step SQL queries that reference themselves for recursion
# MAGIC - Perform employee-manager hierarchy expansion with modular query logic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC In order to follow along with this demo, you will need:
# MAGIC * Databricks runtime\(s\): **17.0 or later**. **Do NOT use serverless compute to run this notebook**.
# MAGIC * Cloud resources to support the metastore.
# MAGIC * Access to a Unity-Catalog enabled Databricks workspace with the ability to create catalogs in your metastore
# MAGIC * Permission to query TPCH sample datasets or equivalent data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Classroom Setup
# MAGIC Before starting the demo, run the provided classroom setup script. This script will automatically define configuration variables necessary for the demo and create a personalized, secure data environment for each user in Databricks using Unity Catalog's three-level namespace ensuring isolation, modularity, and easy organization for data workflows.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise: Recursive CTEs
# MAGIC
# MAGIC We’ll create a small employee table inside your user catalog → bronze schema and run a recursive CTE to traverse the hierarchy.

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1: Set Catalog & Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${DA.user_catalog_name};
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Create Sample Employee Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE employee (
# MAGIC   emp_id INT,
# MAGIC   emp_name STRING,
# MAGIC   manager_id INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO employee VALUES
# MAGIC   (1,'Alice',NULL),
# MAGIC   (2,'Bob',1),
# MAGIC   (3,'Carol',1),
# MAGIC   (4,'Dave',3);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 📌 This creates a simple tree:
# MAGIC
# MAGIC Alice (id=1)
# MAGIC
# MAGIC     ├─ Bob (id=2)
# MAGIC
# MAGIC     └─ Carol (id=3)
# MAGIC
# MAGIC     └─ Dave (id=4)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Recursive CTE Query

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH RECURSIVE subordinates AS (
# MAGIC   -- Anchor: start with Alice
# MAGIC   SELECT emp_id, emp_name, manager_id, 1 AS level
# MAGIC   FROM employee
# MAGIC   WHERE emp_id = 1
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Recursive step: get direct reports
# MAGIC   SELECT e.emp_id, e.emp_name, e.manager_id, s.level + 1
# MAGIC   FROM employee e
# MAGIC   JOIN subordinates s 
# MAGIC     ON e.manager_id = s.emp_id
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM subordinates
# MAGIC ORDER BY level, emp_id;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Excpected Output
# MAGIC
# MAGIC | level | emp_id | emp_name | manager_id |
# MAGIC |-------|--------|----------|------------|
# MAGIC | 1     | 1      | Alice    | NULL       |
# MAGIC | 2     | 2      | Bob      | 1          |
# MAGIC | 2     | 3      | Carol    | 1          |
# MAGIC | 3     | 4      | Dave     | 3          |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks SQL does support recursive CTEs, but the error "Recursive CTEs not supported" can occur if you are using a Databricks Runtime version or SQL endpoint that does not support them, or if you are using a feature or syntax not fully supported in your environment. However, your syntax is correct for Databricks SQL, and recursive CTEs are supported in Databricks SQL Warehouse and newer clusters.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional

# COMMAND ----------

employee = spark.table("employee")

from pyspark.sql import functions as F

result = (
    employee
    .filter(F.col("emp_id") == 1)
    .withColumn("level", F.lit(1))
)

current_level = result

while True:
    next_level = (
        employee.alias("e")
        .join(
            current_level.alias("s"),
            F.col("e.manager_id") == F.col("s.emp_id"),
            "inner"
        )
        .select(
            F.col("e.emp_id"),
            F.col("e.emp_name"),
            F.col("e.manager_id"),
            (F.col("s.level") + 1).alias("level")
        )
    )
    if next_level.count() == 0:
        break
    result = result.union(next_level)
    current_level = next_level

display(
    result.orderBy("level", "emp_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up
# MAGIC Run the below cell to delete the unique catalog DA.user_catalog_name created for this user:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS ${DA.user_catalog_name} CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC
# MAGIC In this demo, we explored how to use Recursive Common Table Expressions in Databricks SQL to handle hierarchical data.
# MAGIC By starting with an anchor member and repeatedly joining to subordinates, we built a full reporting chain for a manager. Recursive CTEs are powerful for organization charts, bill-of-materials, graph traversals, and tree-like data structures in Databricks.