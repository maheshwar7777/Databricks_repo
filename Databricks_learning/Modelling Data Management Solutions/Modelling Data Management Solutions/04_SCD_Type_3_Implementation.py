# Databricks notebook source
# MAGIC %md
# MAGIC ## SCD Type-3 Implementation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Introduction:
# MAGIC SCD Type 3 stores a limited amount of history by keeping the previous value(s) in dedicated columns (for example prev_email, prev_address) alongside the current value. Unlike Type 2, Type 3 does not keep a full history (no multi-row history per business key). It's typically used when you only need one-level history (current + previous).
# MAGIC
# MAGIC #### When to use SCD Type‑3:
# MAGIC - You need only the most recent previous value for a small set of attributes (e.g., current and previous address, current and previous status).
# MAGIC - Regulatory or reporting requirements explicitly need the previous value but not full history.
# MAGIC - You want simpler queries and lower storage than Type‑2.
# MAGIC
# MAGIC #### Limitations:
# MAGIC - Keeps only limited history (commonly only one previous version).
# MAGIC - Not suitable if you must audit all historical changes or time travel of many attribute revisions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Objective:

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook demonstrates a step-by-step implementation of Slowly Changing Dimension (SCD) Type 3 in Databricks using Unity Catalog Delta tables covering the below scenarios:
# MAGIC - Use Unity Catalog managed Delta tables (no location-based Delta tables).
# MAGIC - Compare a computed hash of business columns. If the primary key matches and the hash differs → perform the SCD Type-3 update (preserve limited history in a "previous_*" column).
# MAGIC - Show how deletes in the source can be handled (hard delete vs soft delete).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Catalog 

# COMMAND ----------

# DBTITLE 1,Catalog
# MAGIC %sql
# MAGIC create catalog if not exists scd_type_3_catalog;
# MAGIC use catalog scd_type_3_catalog;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Schema

# COMMAND ----------

# DBTITLE 1,Schema
# MAGIC %sql
# MAGIC create schema if not exists scd_type_3_schema;
# MAGIC use scd_type_3_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create source table and insert sample data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS scd_type_3_catalog.scd_type_3_schema.source_customers (
# MAGIC   customer_id INT,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   city STRING,
# MAGIC   updated_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC
# MAGIC INSERT INTO scd_type_3_catalog.scd_type_3_schema.source_customers VALUES
# MAGIC   (1, 'John', 'Doe', 'john@example.com', 'Mumbai', TIMESTAMP '2025-09-01 10:00:00'),
# MAGIC   (2, 'Jane', 'Roe', 'jane@example.com', 'Delhi', TIMESTAMP '2025-09-02 11:00:00'),
# MAGIC   (3, 'Sam', 'Smith','sam@example.com', 'Bengaluru',TIMESTAMP '2025-09-02 12:00:00');
# MAGIC
# MAGIC
# MAGIC SELECT * FROM scd_type_3_catalog.scd_type_3_schema.source_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Target SCD Type-3 table 
# MAGIC SCD Type 3 concept: keep current value and the previous value for selected attribute(s). We will maintain city history as an example: city_current and city_prev. We also store record_hash and is_deleted.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS scd_type_3_catalog.scd_type_3_schema.customers_scd3 (
# MAGIC customer_id INT,
# MAGIC first_name STRING,
# MAGIC last_name STRING,
# MAGIC email STRING,
# MAGIC city_current STRING,
# MAGIC city_prev STRING,
# MAGIC updated_at TIMESTAMP,
# MAGIC record_hash STRING,
# MAGIC last_updated TIMESTAMP,
# MAGIC is_deleted BOOLEAN
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC Notes:
# MAGIC - Only columns you explicitly want limited history for need *_prev columns. For example, city_prev holds the previous city value.
# MAGIC - You may include additional audit columns like etl_batch_id, source_filename.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hash computation (deterministic)
# MAGIC We compute a hash of business columns used to detect content changes. Use coalesce and concat_ws in a stable column order.

# COMMAND ----------

"sha2(concat_ws('||', coalesce(first_name,''), coalesce(last_name,''), coalesce(email,''), coalesce(city,'')), 256) as record_hash"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initial load into SCD Type-3 table
# MAGIC On initial load, set city_current = city and city_prev = NULL.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO scd_type_3_catalog.scd_type_3_schema.customers_scd3
# MAGIC SELECT
# MAGIC customer_id,
# MAGIC first_name,
# MAGIC last_name,
# MAGIC email,
# MAGIC city AS city_current,
# MAGIC NULL AS city_prev,
# MAGIC updated_at,
# MAGIC sha2(concat_ws('||', coalesce(first_name,''), coalesce(last_name,''), coalesce(email,''), coalesce(city,'')), 256) AS record_hash,
# MAGIC current_timestamp() AS last_updated,
# MAGIC false AS is_deleted
# MAGIC FROM scd_type_3_catalog.scd_type_3_schema.source_customers;
# MAGIC
# MAGIC
# MAGIC SELECT * FROM scd_type_3_catalog.scd_type_3_schema.customers_scd3 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert / merge logic — SCD Type 3 rules
# MAGIC - If customer_id matches and record_hash differs, update dimension columns.
# MAGIC - For attributes tracked for SCD3 (here: city), set city_prev = city_current and city_current = src.city.
# MAGIC - For other attributes not tracked historically (e.g., email), simply overwrite (Type-1 style for non-tracked attributes).
# MAGIC - If customer_id not present in target, insert as new row.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO scd_type_3_catalog.scd_type_3_schema.customers_scd3 AS tgt
# MAGIC USING (
# MAGIC    SELECT *,
# MAGIC    sha2(concat_ws('||', coalesce(first_name,''), coalesce(last_name,''), coalesce(email,''), coalesce(city,'')), 256) AS src_hash
# MAGIC     FROM scd_type_3_catalog.scd_type_3_schema.source_customers
# MAGIC ) AS src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC WHEN MATCHED AND tgt.record_hash <> src.src_hash THEN
# MAGIC  UPDATE SET
# MAGIC    first_name = src.first_name,
# MAGIC    last_name = src.last_name,
# MAGIC    email = src.email,
# MAGIC -- SCD Type-3 behavior for 'city'
# MAGIC    city_prev = tgt.city_current,
# MAGIC    city_current = src.city,
# MAGIC    updated_at = src.updated_at,
# MAGIC    record_hash = src.src_hash,
# MAGIC    last_updated = current_timestamp(),
# MAGIC    is_deleted = false
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT (customer_id, first_name, last_name, email, city_current, city_prev, updated_at, record_hash, last_updated, is_deleted)
# MAGIC    VALUES (src.customer_id, src.first_name, src.last_name, src.email, src.city, NULL, src.updated_at, src.src_hash, current_timestamp(), false);

# COMMAND ----------

# MAGIC %md
# MAGIC Important detail:
# MAGIC - city_prev = tgt.city_current captures the current value prior to the update. This gives you one-step history.
# MAGIC - If you need to track multiple previous values, add city_prev2 etc. — but SCD3 is intended for a small, fixed number of previous values only.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling deletes
# MAGIC Same two options as SCD1:
# MAGIC - Option A — Hard delete

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM scd_type_3_catalog.scd_type_3_schema.customers_scd3
# MAGIC WHERE customer_id NOT IN (SELECT customer_id FROM scd_type_3_catalog.scd_type_3_schema.source_customers);

# COMMAND ----------

# MAGIC %md
# MAGIC Option B — Soft delete (recommended for auditability)
# MAGIC - Mark records not present in the source as deleted.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO scd_type_3_catalog.scd_type_3_schema.customers_scd3 AS tgt
# MAGIC USING (SELECT customer_id FROM scd_type_3_catalog.scd_type_3_schema.source_customers) AS src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC UPDATE SET is_deleted = true, last_updated = current_timestamp();

# COMMAND ----------

# MAGIC %md
# MAGIC When using soft-delete, consider whether city_prev should be changed — usually you leave history intact and just mark the row.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example end-to-end scenario
# MAGIC Simulate update, insert, delete and run the flow:
# MAGIC - Update customer 1's city from Mumbai to Pune (should set city_prev = Mumbai, city_current = Pune).
# MAGIC - Insert customer 4.
# MAGIC - Delete customer 2 from source.
# MAGIC - Run MERGE.
# MAGIC - Apply soft-delete or hard-delete logic.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Update
# MAGIC UPDATE scd_type_3_catalog.scd_type_3_schema.source_customers
# MAGIC SET city = 'Pune', updated_at = current_timestamp()
# MAGIC WHERE customer_id = 1;
# MAGIC
# MAGIC
# MAGIC -- 2. Insert
# MAGIC INSERT INTO scd_type_3_catalog.scd_type_3_schema.source_customers VALUES
# MAGIC (4, 'Priya', 'Kumar', 'priya@example.com', 'Chennai', current_timestamp());
# MAGIC
# MAGIC
# MAGIC -- 3. Delete
# MAGIC DELETE FROM scd_type_3_catalog.scd_type_3_schema.source_customers WHERE customer_id = 2;
# MAGIC
# MAGIC
# MAGIC -- 4) Run MERGE 
# MAGIC MERGE INTO scd_type_3_catalog.scd_type_3_schema.customers_scd3 AS tgt
# MAGIC USING (
# MAGIC    SELECT *,
# MAGIC    sha2(concat_ws('||', coalesce(first_name,''), coalesce(last_name,''), coalesce(email,''), coalesce(city,'')), 256) AS src_hash
# MAGIC     FROM scd_type_3_catalog.scd_type_3_schema.source_customers
# MAGIC ) AS src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC WHEN MATCHED AND tgt.record_hash <> src.src_hash THEN
# MAGIC  UPDATE SET
# MAGIC    first_name = src.first_name,
# MAGIC    last_name = src.last_name,
# MAGIC    email = src.email,
# MAGIC -- SCD Type-3 behavior for 'city'
# MAGIC    city_prev = tgt.city_current,
# MAGIC    city_current = src.city,
# MAGIC    updated_at = src.updated_at,
# MAGIC    record_hash = src.src_hash,
# MAGIC    last_updated = current_timestamp(),
# MAGIC    is_deleted = false
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT (customer_id, first_name, last_name, email, city_current, city_prev, updated_at, record_hash, last_updated, is_deleted)
# MAGIC    VALUES (src.customer_id, src.first_name, src.last_name, src.email, src.city, NULL, src.updated_at, src.src_hash, current_timestamp(), false);
# MAGIC
# MAGIC -- 5) Handle deletes (choose either hard/soft)
# MAGIC MERGE INTO scd_type_3_catalog.scd_type_3_schema.customers_scd3 AS tgt
# MAGIC USING (SELECT customer_id FROM scd_type_3_catalog.scd_type_3_schema.source_customers) AS src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC UPDATE SET is_deleted = true, last_updated = current_timestamp();
# MAGIC
# MAGIC
# MAGIC SELECT * FROM scd_type_3_catalog.scd_type_3_schema.customers_scd3 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC You should observe:
# MAGIC - customer_id = 1: city_prev = 'Mumbai', city_current = 'Pune'.
# MAGIC - customer_id = 4: newly inserted with city_prev = NULL.
# MAGIC - customer_id = 2: either removed (hard-delete) or is_deleted = true (soft-delete).