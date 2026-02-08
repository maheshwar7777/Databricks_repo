# Databricks notebook source
# MAGIC %md
# MAGIC ## Points to be cover:-
# MAGIC - What is Slowly Changing Dimension(SCD)
# MAGIC - Overview of SCD Type-1
# MAGIC - Sample Dataset – Source & Target
# MAGIC - SCD Type 1 – Implementation
# MAGIC - Data Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is SCD
# MAGIC - SCD stands for Slowly Changing Dimension. It’s a data warehousing concept used to manage and track changes in dimension tables over time. Or we can say that it is used to track changes in data over time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview
# MAGIC - Slowly Changing Dimension (SCD) Type 1 in Delta Lake using PySpark in Databricks ensures that if a record already exists in the dimension table, its attributes are updated (overwritten) with the latest data if not exists then data will be insert into target table. No history is preserved.
# MAGIC - SCD Type 1 is used when we want to update the existing record with new information and do not retain the old values.
# MAGIC - Use Case: Correcting typos or errors.
# MAGIC - Example: Changing a customer’s address or name.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Catalog

# COMMAND ----------

# DBTITLE 1,Catalog Creation
# MAGIC %sql
# MAGIC ---Catalog----
# MAGIC create catalog if not exists scd_type_1_catalog;
# MAGIC use catalog scd_type_1_catalog;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Schema

# COMMAND ----------

# DBTITLE 1,Schema creation
# MAGIC %sql
# MAGIC ---Schema----
# MAGIC create schema if not exists scd_type_1_schema;
# MAGIC use schema scd_type_1_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create source table & insert sample data 

# COMMAND ----------

# DBTITLE 1,Create  source table
# MAGIC %sql
# MAGIC ---Create table----
# MAGIC CREATE TABLE IF NOT EXISTS scd_type_1_catalog.scd_type_1_schema.source_customers (
# MAGIC  customer_id INT,
# MAGIC  first_name STRING,
# MAGIC  last_name STRING,
# MAGIC  email STRING,
# MAGIC  city STRING,
# MAGIC  updated_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC
# MAGIC -- insert initial sample rows
# MAGIC INSERT INTO scd_type_1_catalog.scd_type_1_schema.source_customers VALUES
# MAGIC  (1, 'John', 'Doe', 'john@example.com', 'Mumbai', TIMESTAMP '2025-09-01 10:00:00'), 
# MAGIC  (2, 'Jane', 'Roe', 'jane@example.com', 'Delhi', TIMESTAMP '2025-09-02 11:00:00'),
# MAGIC  (3, 'Sam', 'Smith','sam@example.com', 'Bengaluru',TIMESTAMP '2025-09-02 12:00:00');
# MAGIC
# MAGIC
# MAGIC SELECT * FROM scd_type_1_catalog.scd_type_1_schema.source_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Target/Dimension Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS scd_type_1_catalog.scd_type_1_schema.customers_scd1 (
# MAGIC customer_id INT,
# MAGIC first_name STRING,
# MAGIC last_name STRING,
# MAGIC email STRING,
# MAGIC city STRING,
# MAGIC updated_at TIMESTAMP,
# MAGIC record_hash STRING,
# MAGIC last_updated TIMESTAMP,
# MAGIC is_deleted BOOLEAN
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### How we compute the stable hash
# MAGIC Important: Use coalesce to avoid NULL producing NULL hash. Use a deterministic column order and a separator that cannot appear in data (here ||).
# MAGIC
# MAGIC SQL expression used:

# COMMAND ----------

"""sha2(concat_ws('||', coalesce(first_name,''), coalesce(last_name,''), coalesce(email,''), coalesce(city,'')), 256) as record_hash"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initial load into Dimension table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO scd_type_1_catalog.scd_type_1_schema.customers_scd1
# MAGIC SELECT
# MAGIC customer_id,
# MAGIC first_name,
# MAGIC last_name,
# MAGIC email,
# MAGIC city,
# MAGIC updated_at,
# MAGIC sha2(concat_ws('||', coalesce(first_name,''), coalesce(last_name,''), coalesce(email,''), coalesce(city,'')), 256) AS record_hash,
# MAGIC current_timestamp() AS last_updated,
# MAGIC false AS is_deleted
# MAGIC FROM scd_type_1_catalog.scd_type_1_schema.source_customers;
# MAGIC
# MAGIC
# MAGIC -- Validate
# MAGIC SELECT * FROM scd_type_1_catalog.scd_type_1_schema.customers_scd1 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert logic: using MERGE with hash comparison
# MAGIC Logic: If customer_id exists and record_hash changed → UPDATE columns (Type-1 behavior: overwrite). If customer_id doesn't exist → INSERT.

# COMMAND ----------

# DBTITLE 1,New Logic
# MAGIC %sql
# MAGIC -- Use your UC location
# MAGIC USE CATALOG scd_type_1_catalog;
# MAGIC USE SCHEMA  scd_type_1_schema;
# MAGIC
# MAGIC -- MERGE: upsert last-5-days & delete (or soft-delete) unmatched rows from that window
# MAGIC MERGE INTO scd_type_1_catalog.scd_type_1_schema.customers_scd1 AS tgt
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     city,
# MAGIC     updated_at,
# MAGIC     -- hash of non-PK business fields
# MAGIC     sha2(
# MAGIC       concat_ws('||',
# MAGIC         coalesce(first_name,''), coalesce(last_name,''),
# MAGIC         coalesce(email,''),      coalesce(city,'')
# MAGIC       ),
# MAGIC       256
# MAGIC     ) AS src_hash
# MAGIC   FROM scd_type_1_catalog.scd_type_1_schema.source_customers
# MAGIC   WHERE updated_at >= date_sub(current_date(), 5)   --  only last 5 days from source
# MAGIC ) AS src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC
# MAGIC -- UPDATE only if something really changed (hash differs)
# MAGIC WHEN MATCHED AND tgt.record_hash <> src.src_hash THEN
# MAGIC   UPDATE SET
# MAGIC     first_name  = src.first_name,
# MAGIC     last_name   = src.last_name,
# MAGIC     email       = src.email,
# MAGIC     city        = src.city,
# MAGIC     updated_at  = src.updated_at,
# MAGIC     record_hash = src.src_hash,
# MAGIC     last_updated = current_timestamp(),
# MAGIC     is_deleted   = false
# MAGIC
# MAGIC -- INSERT brand-new customers from the 5-day source window
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (customer_id, first_name, last_name, email, city, updated_at, record_hash, last_updated, is_deleted)
# MAGIC   VALUES (src.customer_id, src.first_name, src.last_name, src.email, src.city, src.updated_at, src.src_hash, current_timestamp(), false)
# MAGIC
# MAGIC -- OPTION A: HARD DELETE unmatched target rows from the same 5-day window
# MAGIC WHEN NOT MATCHED BY SOURCE
# MAGIC      AND tgt.updated_at >= date_sub(current_date(), 5) THEN
# MAGIC   DELETE;
# MAGIC
# MAGIC --  OPTION B: SOFT DELETE instead of hard delete (use this clause INSTEAD of the DELETE above)
# MAGIC -- WHEN NOT MATCHED BY SOURCE
# MAGIC --      AND tgt.updated_at >= date_sub(current_date(), 5) THEN
# MAGIC --   UPDATE SET is_deleted = true, last_updated = current_timestamp();
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO scd_type_1_catalog.scd_type_1_schema.customers_scd1 AS tgt
# MAGIC USING (
# MAGIC     SELECT *,
# MAGIC     sha2(concat_ws('||', coalesce(first_name,''), coalesce(last_name,''), coalesce(email,''), coalesce(city,'')), 256) AS src_hash
# MAGIC     FROM scd_type_1_catalog.scd_type_1_schema.source_customers
# MAGIC     ---where updated_at >= date_sub(current_date(), 5)
# MAGIC ) AS src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC WHEN MATCHED AND tgt.record_hash <> src.src_hash THEN
# MAGIC UPDATE SET
# MAGIC    first_name = src.first_name,
# MAGIC    last_name = src.last_name,
# MAGIC    email = src.email,
# MAGIC    city = src.city,
# MAGIC    updated_at = src.updated_at,
# MAGIC    record_hash = src.src_hash,
# MAGIC    last_updated = current_timestamp(),
# MAGIC    is_deleted = false
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT (customer_id, first_name, last_name, email, city, updated_at, record_hash, last_updated, is_deleted)
# MAGIC    VALUES (src.customer_id, src.first_name, src.last_name, src.email, src.city, src.updated_at, src.src_hash, current_timestamp(), false)
# MAGIC
# MAGIC --  HARD DELETE unmatched target rows within the same 5-day window: 
# MAGIC
# MAGIC WHEN NOT MATCHED BY SOURCE
# MAGIC      AND tgt.updated_at >= date_sub(current_date(), 5) THEN
# MAGIC   DELETE;
# MAGIC
# MAGIC -- SOFT DELETE alternative (use this INSTEAD of the DELETE above):
# MAGIC -- WHEN NOT MATCHED BY SOURCE
# MAGIC --      AND tgt.updated_at >= date_sub(current_date(), 5) THEN
# MAGIC --   UPDATE SET is_deleted = true, last_updated = current_timestamp(); 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO scd_type_1_catalog.scd_type_1_schema.customers_scd1 AS tgt
# MAGIC USING (
# MAGIC     SELECT *,
# MAGIC     sha2(concat_ws('||', coalesce(first_name,''), coalesce(last_name,''), coalesce(email,''), coalesce(city,'')), 256) AS src_hash
# MAGIC     FROM scd_type_1_catalog.scd_type_1_schema.source_customers
# MAGIC     ---where updated_at >= date_sub(current_date(), 5)
# MAGIC ) AS src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC WHEN MATCHED AND tgt.record_hash <> src.src_hash THEN
# MAGIC UPDATE SET
# MAGIC    first_name = src.first_name,
# MAGIC    last_name = src.last_name,
# MAGIC    email = src.email,
# MAGIC    city = src.city,
# MAGIC    updated_at = src.updated_at,
# MAGIC    record_hash = src.src_hash,
# MAGIC    last_updated = current_timestamp(),
# MAGIC    is_deleted = false
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT (customer_id, first_name, last_name, email, city, updated_at, record_hash, last_updated, is_deleted)
# MAGIC    VALUES (src.customer_id, src.first_name, src.last_name, src.email, src.city, src.updated_at, src.src_hash, current_timestamp(), false);
# MAGIC
# MAGIC --  HARD DELETE unmatched target rows within the same 5-day window:
# MAGIC WHEN NOT MATCHED BY SOURCE
# MAGIC      AND tgt.updated_at >= date_sub(current_date(), 5) THEN
# MAGIC   DELETE;
# MAGIC
# MAGIC -- SOFT DELETE alternative (use this INSTEAD of the DELETE above):
# MAGIC -- WHEN NOT MATCHED BY SOURCE
# MAGIC --      AND tgt.updated_at >= date_sub(current_date(), 5) THEN
# MAGIC --   UPDATE SET is_deleted = true, last_updated = current_timestamp(); 

# COMMAND ----------

# MAGIC %md
# MAGIC Notes:
# MAGIC - Above the WHEN MATCHED clause includes tgt.record_hash <> src.src_hash so we only update when the content changed.
# MAGIC - Because this is SCD Type-1, we overwrite/update the dimension columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling deletes scenario in source table — Two approaches
# MAGIC - When a record disappears from the source, you must choose how you want the SCD Type-1 table to look:
# MAGIC - Option A — Hard delete (target must exactly mirror source):-
# MAGIC If you want the target to match the source exactly (deleted rows in source should be removed in the target):

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete rows in SCD target that are no longer present in the source
# MAGIC DELETE FROM scd_type_1_catalog.scd_type_1_schema.customers_scd1
# MAGIC WHERE customer_id NOT IN (
# MAGIC SELECT customer_id FROM scd_type_1_catalog.scd_type_1_schema.source_customers
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B — Soft delete (recommended for auditability)
# MAGIC Keep deleted rows in the target but mark them as inactive/soft-deleted. This is useful for reporting and auditing.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Soft-delete: flag rows that are not present in the source
# MAGIC MERGE INTO scd_type_1_catalog.scd_type_1_schema.customers_scd1 AS tgt
# MAGIC USING (
# MAGIC SELECT customer_id FROM scd_type_1_catalog.scd_type_1_schema.source_customers
# MAGIC ) AS src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC UPDATE SET is_deleted = true, last_updated = current_timestamp();
# MAGIC
# MAGIC
# MAGIC -- or using an anti-join approach (equivalent):
# MAGIC UPDATE scd_type_1_catalog.scd_type_1_schema.customers_scd1
# MAGIC SET is_deleted = true, last_updated = current_timestamp()
# MAGIC WHERE customer_id NOT IN (SELECT customer_id FROM scd_type_1_catalog.scd_type_1_schema.source_customers);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: simulate update / insert / delete and run an end-to-end flow
# MAGIC - Modify source: update a row's email (simulate change)
# MAGIC - Insert a new row (simulate new customer)
# MAGIC - Delete a row from the source (simulate deletion)
# MAGIC - Run the MERGE upsert
# MAGIC - Apply delete-handling scenario
# MAGIC - Validate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1) Update customer 1
# MAGIC UPDATE scd_type_1_catalog.scd_type_1_schema.source_customers
# MAGIC SET email = 'john.doe@new.com', updated_at = current_timestamp()
# MAGIC WHERE customer_id = 1;
# MAGIC
# MAGIC
# MAGIC -- 2) Insert new customer 4
# MAGIC INSERT INTO scd_type_1_catalog.scd_type_1_schema.source_customers VALUES
# MAGIC (4, 'Priya', 'Kumar', 'priya@example.com', 'Chennai', current_timestamp());
# MAGIC
# MAGIC
# MAGIC -- 3) Delete customer 2 from source (simulate deletion)
# MAGIC DELETE FROM scd_type_1_catalog.scd_type_1_schema.source_customers WHERE customer_id = 2;
# MAGIC
# MAGIC
# MAGIC -- 4) Run the MERGE upsert 
# MAGIC MERGE INTO scd_type_1_catalog.scd_type_1_schema.customers_scd1 AS tgt
# MAGIC USING (
# MAGIC    SELECT *,
# MAGIC    sha2(concat_ws('||', coalesce(first_name,''), coalesce(last_name,''), coalesce(email,''), coalesce(city,'')), 256) AS src_hash
# MAGIC    FROM scd_type_1_catalog.scd_type_1_schema.source_customers
# MAGIC ) AS src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC WHEN MATCHED AND tgt.record_hash <> src.src_hash THEN
# MAGIC UPDATE SET
# MAGIC    first_name = src.first_name,
# MAGIC    last_name = src.last_name,
# MAGIC    email = src.email,
# MAGIC    city = src.city,
# MAGIC    updated_at = src.updated_at,
# MAGIC    record_hash = src.src_hash,
# MAGIC    last_updated = current_timestamp(),
# MAGIC    is_deleted = false
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT (customer_id, first_name, last_name, email, city, updated_at, record_hash, last_updated, is_deleted)
# MAGIC    VALUES (src.customer_id, src.first_name, src.last_name, src.email, src.city, src.updated_at, src.src_hash, current_timestamp(), false);
# MAGIC
# MAGIC
# MAGIC -- 5) Handle deletes (choose one): hard-delete or soft-delete (see section 7)
# MAGIC -- Delete: flag rows that are not present in the source
# MAGIC MERGE INTO scd_type_1_catalog.scd_type_1_schema.customers_scd1 AS tgt
# MAGIC USING (
# MAGIC    SELECT customer_id FROM scd_type_1_catalog.scd_type_1_schema.source_customers
# MAGIC ) AS src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC    UPDATE SET is_deleted = true, last_updated = current_timestamp();
# MAGIC
# MAGIC -- or using an anti-join approach (equivalent):
# MAGIC UPDATE scd_type_1_catalog.scd_type_1_schema.customers_scd1
# MAGIC SET is_deleted = true, last_updated = current_timestamp()
# MAGIC WHERE customer_id NOT IN (SELECT customer_id FROM scd_type_1_catalog.scd_type_1_schema.source_customers);
# MAGIC
# MAGIC -- 6) Validate
# MAGIC SELECT * FROM scd_type_1_catalog.scd_type_1_schema.customers_scd1 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Here you will observe:
# MAGIC - customer_id = 1 updated with new email and new record_hash
# MAGIC - customer_id = 4 inserted
# MAGIC - customer_id = 2 either deleted or flagged is_deleted = true

# COMMAND ----------

spark