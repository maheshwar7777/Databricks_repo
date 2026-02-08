-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Working with Materialized Views in Databricks SQL
-- MAGIC
-- MAGIC This notebook demonstrates how to create, configure, and refresh materialized views using Databricks SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Setup and Prerequisites
-- MAGIC Ensure you're using a Unity Catalog-enabled SQL warehouse and have the necessary permissions:
-- MAGIC - SELECT on base tables
-- MAGIC - USE CATALOG and USE SCHEMA on source and target schemas
-- MAGIC - CREATE MATERIALIZED VIEW privilege on the target schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For a user to be able to refresh the MV, they require:
-- MAGIC
-- MAGIC - USE CATALOG privilege on the parent catalog and the USE SCHEMA privilege on the parent schema.
-- MAGIC - Ownership of the MV or REFRESH privilege on the MV.
-- MAGIC - The owner of the MV must have the SELECT privilege over the base tables referenced by the MV.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For a user to be able to query the MV, they require:
-- MAGIC
-- MAGIC - USE CATALOG privilege on the parent catalog and the USE SCHEMA privilege on the parent schema.
-- MAGIC - SELECT privilege over the materialized view.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Create a Materialized View

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW sales_summary_sample_mv
COMMENT 'A summary of sales data with a new total count.'
AS SELECT
  sale_date,
  region,
  SUM(sale_amount) AS total_sales,
  COUNT(sale_amount) AS total_transactions -- New column added here
FROM
  data_streaming.default.sales_data
GROUP BY
  sale_date,
  region;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Parameters

-- COMMAND ----------

--REPLACE - replaces the view and its content if it already exists.
CREATE OR REPLACE MATERIALIZED VIEW my_view AS SELECT * FROM sales_data;

--IF NOT EXISTS - Creates the view only if it does not exist.
CREATE MATERIALIZED VIEW IF NOT EXISTS my_view_test AS SELECT * FROM sales_data;

--column_list - Optionally labels the columns in the query result of the view.
CREATE MATERIALIZED VIEW my_view1 (
  region STRING COMMENT 'Region',
  sale_amount DOUBLE COMMENT 'Total amount',
  sale_date DATE COMMENT 'Sale Date'
) AS SELECT region, sale_amount,sale_date FROM sales_data;

--MASK clause - Adds a column mask function to anonymize sensitive data.
-- CREATE MATERIALIZED VIEW my_view (
--   email STRING MASK(CASE WHEN CURRENT_USER() = 'admin' THEN email ELSE '***' END)
-- ) AS SELECT email FROM users;

--table_constraint - Adds an informational primary or foreign key constraint to the table.
--PARTITIONED BY - Optionally partitions the table by specified columns.
CREATE MATERIALIZED VIEW my_view2 PARTITIONED BY (region) AS SELECT * FROM sales_data;

--CLUSTER BY - Optionally clusters by a subset of columns
CREATE MATERIALIZED VIEW my_view3 CLUSTER BY (region) AS SELECT * FROM sales_data;

--COMMENT - Adds a description to the view.
CREATE MATERIALIZED VIEW my_view4 COMMENT 'Monthly sales summary' AS SELECT * FROM sales_data;

--DEFAULT COLLATION UTF8_BINARY - Sets the default collation for the view.
--TBLPROPERTIES - Sets user-defined properties for the view.
CREATE MATERIALIZED VIEW my_view5 TBLPROPERTIES ('property_key' = 'property_value') AS SELECT * FROM sales_data;

--SCHEDULE - Schedules automatic refreshes for the materialized view.
  CREATE MATERIALIZED VIEW my_view6
  SCHEDULE REFRESH EVERY 1 DAY
  AS SELECT * FROM sales_data;

  --using CRON
  
  CREATE MATERIALIZED VIEW my_view7
  SCHEDULE REFRESH CRON '0 0 * * 1' AT TIME ZONE 'UTC'
  AS SELECT * FROM sales_data;


--TRIGGER ON UPDATE -(Beta) Refreshes the view when upstream data changes, at most once per specified interval.

--WITH ROW FILTER clause - Adds a row filter function for fine-grained access control.

--AS query - The SQL query that defines the materialized view.





-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Schedule Automatic Refresh

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This SQL snippet creates a materialized view named sales_summary_mv that:
-- MAGIC
-- MAGIC - Aggregates sales data from the sales_data table.
-- MAGIC - Groups the data by sale_date and region.
-- MAGIC - Calculates the total sales amount (SUM(sale_amount)) for each date-region combination.
-- MAGIC - Refreshes automatically every 1 day, ensuring the view stays up-to-date with the latest data.
-- MAGIC - Materialized views store the query result physically, improving performance for repeated access to summary data.

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW sales_summary_mv2
SCHEDULE REFRESH EVERY 1 DAY
AS
SELECT
  sale_date,
  region,
  SUM(sale_amount) AS total_sales
FROM
  data_streaming.default.sales_data
GROUP BY
  sale_date, region;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Add Row Filters and Column Masks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This SQL snippet creates a materialized view named sensitive_sales_mv with security features:
-- MAGIC
-- MAGIC - Row Filter: Only users who are members of the 'FinanceTeam' group can access the rows. This ensures role-based access control.
-- MAGIC - Column Masking: The sale_amount column is masked using the MASK() function, displaying 'MASKED' instead of the actual value. This protects sensitive financial data from unauthorized viewing.
-- MAGIC - The view includes sale_date, region, and the masked version of sale_amount.

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW sensitive_sales_mv
WITH ROW FILTER data_streaming.default.row_filter
AS
SELECT
sale_date,
region,
data_streaming.default.column_mask(sale_amount) as sale_amount
FROM
data_streaming.default.sales_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Limitations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - When a materialized view with a sum aggregate over a NULL-able column has the last non-NULL value removed from that column - and thus only NULL values remain in that column - the materialized view's resultant aggregate value returns zero instead of NULL.
-- MAGIC - Column-reference does not require an alias. Non-column reference expressions require an alias, as in the following example:
-- MAGIC - Allowed: SELECT col1, SUM(col2) AS sum_col2 FROM t GROUP BY col1
-- MAGIC - Not Allowed: SELECT col1, SUM(col2) FROM t GROUP BY col1
-- MAGIC - NOT NULL must be manually specified along with PRIMARY KEY in order to be a valid statement.
-- MAGIC - Materialized views do not support identity columns or surrogate keys.
-- MAGIC - Materialized views do not support OPTIMIZE and VACUUM commands. Maintenance happens automatically.
-- MAGIC Materialized views do not support expectations to define data quality constraints.