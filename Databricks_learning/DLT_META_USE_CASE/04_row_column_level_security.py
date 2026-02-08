# Databricks notebook source
# MAGIC %md
# MAGIC ###  **Agenda :**
# MAGIC
# MAGIC Implementing Data Security in Databricks with Unity Catalog.
# MAGIC
# MAGIC ### **Objective :**
# MAGIC To learn how to secure sensitive data in Databricks using:
# MAGIC
# MAGIC Row-Level Security (RLS) – filtering data dynamically.
# MAGIC
# MAGIC Column-Level Security (Masking) – hiding or partially showing sensitive fields.

# COMMAND ----------

# MAGIC %md
# MAGIC ###  **Lab Flow**
# MAGIC ### 
# MAGIC ###  Row level security 
# MAGIC
# MAGIC 1.Create Secure Table
# MAGIC
# MAGIC 2.Generate secure_top5_customers table (top 5 spenders).
# MAGIC
# MAGIC 3.Apply Row-Level Security
# MAGIC
# MAGIC 4.Define and attach a row filter function.
# MAGIC
# MAGIC 5.Demonstrate before and after effect of RLS.
# MAGIC
# MAGIC ###  Column level security
# MAGIC
# MAGIC 1.Apply Column-Level Security
# MAGIC
# MAGIC 2.Partial masking: e.g., show only first 3 characters of email, mask the rest (abc****@gmail.com).
# MAGIC
# MAGIC 3.Full masking: completely hide sensitive column (replace with XXXXXX).
# MAGIC
# MAGIC 4.Validation
# MAGIC
# MAGIC 5.Query data with and without filters to verify.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Row Level security using UDF

# COMMAND ----------

# DBTITLE 1,Create Secure Table (Top 5 Customers)
# MAGIC %sql
# MAGIC -- RLS using UDF
# MAGIC -->identify top 5 customers with highest transaction value  -- rls on email column
# MAGIC
# MAGIC -- drop table dlt_catalog.dlt_meta.secure_top10_customers
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dlt_catalog.dlt_meta.secure_top10_customers
# MAGIC AS
# MAGIC
# MAGIC WITH customer_spend AS (
# MAGIC     SELECT 
# MAGIC         c.customer_id,
# MAGIC         c.name,
# MAGIC         c.email,
# MAGIC         SUM(t.total_price) AS total_spent
# MAGIC     FROM dlt_catalog.dlt_meta.silver_customer c
# MAGIC     INNER JOIN dlt_catalog.dlt_meta.silver_transaction t
# MAGIC         ON c.customer_id = t.customer_id
# MAGIC     GROUP BY c.customer_id, c.name, c.email
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM customer_spend
# MAGIC ORDER BY total_spent DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Query Before Applying RLS
# MAGIC %sql
# MAGIC -- before row level filter
# MAGIC select * from dlt_catalog.dlt_meta.secure_top10_customers;

# COMMAND ----------

# DBTITLE 1,Create and Apply Row Filter Function
# MAGIC %sql
# MAGIC
# MAGIC -- drop function dlt_catalog.dlt_meta.row_filter_top_10_customers_net
# MAGIC
# MAGIC CREATE or REPLACE FUNCTION dlt_catalog.dlt_meta.row_filter_top_10_customers_net(email STRING)
# MAGIC RETURN (SUBSTRING_INDEX(email, '.', -1) = 'net');
# MAGIC
# MAGIC ALTER TABLE dlt_catalog.dlt_meta.secure_top10_customers
# MAGIC SET ROW FILTER dlt_catalog.dlt_meta.row_filter_top_10_customers_net ON (email);

# COMMAND ----------

# DBTITLE 1,Query After Applying RLS
# MAGIC %sql
# MAGIC -- After row level filter
# MAGIC select * from dlt_catalog.dlt_meta.secure_top10_customers;

# COMMAND ----------

# DBTITLE 1,RLS via view
# MAGIC %sql
# MAGIC -- RLS via view using where clause
# MAGIC CREATE OR REPLACE VIEW dlt_catalog.dlt_meta.secure_customer_view AS
# MAGIC SELECT * 
# MAGIC FROM dlt_catalog.dlt_meta.secure_top10_customers
# MAGIC WHERE SUBSTRING_INDEX(email, '.', -1) = 'net';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Row filter with UDF 
# MAGIC -  This is centralized, reusable, enforceable at the table level. Even if user queries the base table, policy is enforced.
# MAGIC
# MAGIC ### 2. Why use RLS vs WHERE clause?
# MAGIC
# MAGIC - WHERE clause works only if user remembers to add it. Security shouldn’t rely on developer discipline.
# MAGIC - Row filters (RLS) are centrally enforced policies → guaranteed across all queries, dashboards, or notebooks.
# MAGIC - Governance/compliance: auditors expect consistent rules.
# MAGIC - Easier maintenance: one filter function can apply to multiple tables.

# COMMAND ----------

# DBTITLE 1,Static filter (same for everyone)
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION static_filter(email STRING)
# MAGIC RETURN (SUBSTRING_INDEX(email, '.', -1) = 'net');
# MAGIC
# MAGIC ALTER TABLE dlt_catalog.dlt_meta.secure_top10_customers
# MAGIC SET ROW FILTER static_filter ON (email);
# MAGIC

# COMMAND ----------

# DBTITLE 1,Dynamic filter (depends on user/session)
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION dynamic_filter(email STRING)
# MAGIC RETURN (email = current_user());
# MAGIC
# MAGIC ALTER TABLE dlt_catalog.dlt_meta.secure_top10_customers
# MAGIC SET ROW FILTER dynamic_filter ON (email);

# COMMAND ----------

# MAGIC %md
# MAGIC  Static filter = simple, optimized, predicate pushdown possible.
# MAGIC
# MAGIC  Dynamic filter = flexible, but can be less optimized because it evaluates per user/session.
# MAGIC  They will only see rows where the email column = their own login.
# MAGIC  This is dynamic Row Level Security (RLS), because the rows returned depend on who is querying.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Column Level security

# COMMAND ----------

# DBTITLE 1,Partial Masking UDF function
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION dlt_catalog.dlt_meta.mask_quantity(val INT)
# MAGIC RETURN 
# MAGIC     CASE
# MAGIC         WHEN current_user() IN ('admin', 'auditor') THEN CAST(val AS STRING)  -- full view
# MAGIC         ELSE CONCAT('XX', RIGHT(CAST(val AS STRING), 1))                     -- partial mask
# MAGIC     END;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Applying masking udf function
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW dlt_catalog.dlt_meta.secure_top5_products AS
# MAGIC WITH product_sales AS (
# MAGIC     SELECT 
# MAGIC         p.product_id,
# MAGIC         p.product_name,
# MAGIC         SUM(t.quantity) AS total_quantity
# MAGIC     FROM dlt_catalog.dlt_meta.silver_transaction t
# MAGIC     JOIN dlt_catalog.dlt_meta.silver_product p
# MAGIC       ON t.product_id = p.product_id
# MAGIC     GROUP BY p.product_id, p.product_name
# MAGIC )
# MAGIC SELECT 
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     dlt_catalog.dlt_meta.mask_quantity(total_quantity) AS masked_quantity
# MAGIC FROM product_sales
# MAGIC ORDER BY total_quantity DESC
# MAGIC LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt_catalog.dlt_meta.secure_top5_products

# COMMAND ----------

# DBTITLE 1,Full Masking UDF
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION dlt_catalog.dlt_meta.mask_price(val INT)
# MAGIC RETURN CASE
# MAGIC     WHEN current_user() IN ('finance_user', 'admin', 'auditor') THEN CAST(val AS STRING)  -- full price
# MAGIC     ELSE 'MASKED'                                                                         -- fully masked
# MAGIC END;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Applying full masking
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW dlt_catalog.dlt_meta.secure_top_product_monthly AS
# MAGIC WITH monthly_sales AS (
# MAGIC     SELECT 
# MAGIC         DATE_TRUNC('month', t.transaction_date) AS sales_month,
# MAGIC         p.product_id,
# MAGIC         p.product_name,
# MAGIC         SUM(t.quantity) AS total_quantity,
# MAGIC         p.price
# MAGIC     FROM dlt_catalog.dlt_meta.silver_transaction t
# MAGIC     JOIN dlt_catalog.dlt_meta.silver_product p
# MAGIC       ON t.product_id = p.product_id
# MAGIC     GROUP BY DATE_TRUNC('month', t.transaction_date), p.product_id, p.product_name, p.price
# MAGIC ),
# MAGIC ranked_sales AS (
# MAGIC     SELECT 
# MAGIC         sales_month,
# MAGIC         product_id,
# MAGIC         product_name,
# MAGIC         dlt_catalog.dlt_meta.mask_price(price) AS masked_price,   -- masking applied here
# MAGIC         total_quantity,
# MAGIC         RANK() OVER (PARTITION BY sales_month ORDER BY total_quantity DESC) AS rnk
# MAGIC     FROM monthly_sales
# MAGIC )
# MAGIC SELECT 
# MAGIC     sales_month,
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     masked_price,
# MAGIC     total_quantity
# MAGIC FROM ranked_sales
# MAGIC WHERE rnk = 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  dlt_catalog.dlt_meta.secure_top_product_monthly

# COMMAND ----------

# DBTITLE 1,Column Level Security with group_membership()
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION dlt_catalog.dlt_meta.mask_price(val INT)
# MAGIC RETURN CASE
# MAGIC     WHEN group_membership('finance_team') OR group_membership('admin') THEN CAST(val AS STRING)
# MAGIC     ELSE 'MASKED'
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     dlt_catalog.dlt_meta.mask_price(price) AS secure_price
# MAGIC FROM dlt_catalog.dlt_meta.silver_product;
# MAGIC

# COMMAND ----------

# DBTITLE 1,is_account_group_member in Column Level Security
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION dlt_catalog.dlt_meta.mask_price(val INT)
# MAGIC RETURN CASE
# MAGIC     WHEN is_account_group_member('finance_team') OR is_account_group_member('admin') THEN CAST(val AS STRING)
# MAGIC     ELSE 'MASKED'
# MAGIC END;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     dlt_catalog.dlt_meta.mask_price(price) AS secure_price
# MAGIC FROM dlt_catalog.dlt_meta.silver_product;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - group_membership() → for workspace-level groups (common in most demos, POCs, small orgs).
# MAGIC - is_account_group_member() → for account-wide groups managed via SCIM/Unity Catalog (enterprise setups, multiple workspaces).
# MAGIC - Use is_account_group_member() if your groups are managed centrally in Databricks Account Console (Unity Catalog enabled).
# MAGIC - Use group_membership() if you’re still on workspace-local groups.

# COMMAND ----------

# MAGIC %md
# MAGIC ### # Summary
# MAGIC Row-Level Security (RLS):
# MAGIC
# MAGIC - Static row filter → most optimized (engine can push predicate).
# MAGIC - Dynamic row filter → necessary for personalization, slight tradeoff in performance.
# MAGIC - WHERE clause in view → fast but not secure, ❌ not recommended.
# MAGIC
# MAGIC Column-Level Security (CLS):
# MAGIC
# MAGIC - Masking policies (Unity Catalog) → most optimized + secure.
# MAGIC - Masking functions (UDFs) → good compromise, slightly less performant.
# MAGIC - CASE in query → fast but ❌ not maintainable, not secure.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion :
# MAGIC
# MAGIC By the end of this lab, you will have hands-on experience with:
# MAGIC
# MAGIC 1.Creating secure tables in Databricks.
# MAGIC
# MAGIC 2.Implementing Row Level Security (RLS).
# MAGIC
# MAGIC 3.Implementing Partial & Full Masking policies for sensitive columns.