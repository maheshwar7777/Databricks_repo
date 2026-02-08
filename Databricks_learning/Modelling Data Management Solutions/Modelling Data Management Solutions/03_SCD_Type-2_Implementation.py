# Databricks notebook source
# MAGIC %md
# MAGIC ## SCD Type-2 Implementation in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Introduction
# MAGIC - Slowly Changing Dimensions (SCD) is a common practice in data warehousing to manage and track changes in records over time. SCD Type 2 is particularly useful as it maintains a complete history of changes by creating new rows for each change, allowing for as-was reporting use cases.
# MAGIC - In Data Warehousing, Slowly Changing Dimensions (SCD) handle how data in dimension tables evolves over time.
# MAGIC - SCD Type-2: Preserves historical changes by adding new rows with validity dates & flags.
# MAGIC - Example: If an employee moves to a new department, SCD Type-2 will insert a new row with updated details while keeping the old row active until the change.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Case
# MAGIC - Dimension Table: Customer_Dim
# MAGIC - Source Table: Customer_Source
# MAGIC
# MAGIC  Columns:
# MAGIC - Natural Key: customer_id
# MAGIC - Attributes: customer_name, address, phone_number
# MAGIC - Metadata for SCD2: effective_date, end_date, is_current
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists scd_type_2_catalog;
# MAGIC use catalog scd_type_2_catalog;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists scd_type_2_schema;
# MAGIC use scd_type_2_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Source (business keys + attributes)
# MAGIC CREATE TABLE IF NOT EXISTS scd_type_2_catalog.scd_type_2_schema.source_customers (
# MAGIC   customer_id INT,
# MAGIC   first_name  STRING,
# MAGIC   last_name   STRING,
# MAGIC   email       STRING,
# MAGIC   city        STRING,
# MAGIC   updated_at  TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Seed sample
# MAGIC DELETE FROM scd_type_2_catalog.scd_type_2_schema.source_customers;
# MAGIC INSERT INTO scd_type_2_catalog.scd_type_2_schema.source_customers VALUES
# MAGIC  (1, 'John', 'Doe',  'john@example.com',  'Mumbai',    TIMESTAMP '2025-09-01 10:00:00'),
# MAGIC  (2, 'Jane', 'Roe',  'jane@example.com',  'Delhi',     TIMESTAMP '2025-09-02 11:00:00'),
# MAGIC  (3, 'Sam',  'Smith','sam@example.com',   'Bengaluru', TIMESTAMP '2025-09-02 12:00:00');
# MAGIC
# MAGIC SELECT * FROM scd_type_2_catalog.scd_type_2_schema.source_customers ORDER BY customer_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Target SCD2 table (no locations; UC-managed Delta)
# MAGIC CREATE TABLE IF NOT EXISTS scd_type_2_catalog.scd_type_2_schema.customers_scd2 (
# MAGIC   customer_id   INT,
# MAGIC   first_name    STRING,
# MAGIC   last_name     STRING,
# MAGIC   email         STRING,
# MAGIC   city          STRING,
# MAGIC   updated_at    TIMESTAMP,         -- as received from source
# MAGIC   record_hash   STRING,            -- deterministic hash of non-PK fields
# MAGIC   effective_from TIMESTAMP,        -- SCD2: version start
# MAGIC   effective_to   TIMESTAMP,        -- SCD2: version end (open-ended = 9999-12-31 23:59:59)
# MAGIC   is_current     BOOLEAN,          -- SCD2: current flag
# MAGIC   version        INT,              -- SCD2: version number
# MAGIC   last_updated   TIMESTAMP,        -- audit
# MAGIC   is_deleted     BOOLEAN           -- optional soft-delete flag
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM scd_type_2_catalog.scd_type_2_schema.customers_scd2;
# MAGIC
# MAGIC INSERT INTO scd_type_2_catalog.scd_type_2_schema.customers_scd2
# MAGIC SELECT
# MAGIC   s.customer_id, s.first_name, s.last_name, s.email, s.city, s.updated_at,
# MAGIC   sha2(concat_ws('||',
# MAGIC         coalesce(s.first_name,''), coalesce(s.last_name,''),
# MAGIC         coalesce(s.email,''),      coalesce(s.city,'')), 256)          AS record_hash,
# MAGIC   coalesce(s.updated_at, current_timestamp())                          AS effective_from,
# MAGIC   TIMESTAMP '9999-12-31 23:59:59'                                      AS effective_to,
# MAGIC   true                                                                 AS is_current,
# MAGIC   1                                                                    AS version,
# MAGIC   current_timestamp()                                                  AS last_updated,
# MAGIC   false                                                                AS is_deleted
# MAGIC FROM scd_type_2_catalog.scd_type_2_schema.source_customers s;
# MAGIC
# MAGIC SELECT * FROM scd_type_2_catalog.scd_type_2_schema.customers_scd2 ORDER BY customer_id, version;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step A: expire current rows where attributes changed (hash differs)
# MAGIC MERGE INTO scd_type_2_catalog.scd_type_2_schema.customers_scd2 AS tgt
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     sha2(concat_ws('||',
# MAGIC       coalesce(first_name,''), coalesce(last_name,''),
# MAGIC       coalesce(email,''),      coalesce(city,'')), 256) AS src_hash
# MAGIC   FROM scd_type_2_catalog.scd_type_2_schema.source_customers
# MAGIC ) AS src
# MAGIC ON  tgt.customer_id = src.customer_id
# MAGIC AND tgt.is_current  = true
# MAGIC WHEN MATCHED AND tgt.record_hash <> src.src_hash THEN
# MAGIC   UPDATE SET
# MAGIC     effective_to = current_timestamp(),
# MAGIC     is_current   = false,
# MAGIC     last_updated = current_timestamp();
# MAGIC
# MAGIC -- Step B: insert new current versions for (a) new keys, or (b) changed keys
# MAGIC INSERT INTO scd_type_2_catalog.scd_type_2_schema.customers_scd2
# MAGIC SELECT
# MAGIC   s.customer_id, s.first_name, s.last_name, s.email, s.city, s.updated_at,
# MAGIC   s.src_hash                                       AS record_hash,
# MAGIC   coalesce(s.updated_at, current_timestamp())      AS effective_from,
# MAGIC   TIMESTAMP '9999-12-31 23:59:59'                  AS effective_to,
# MAGIC   true                                             AS is_current,
# MAGIC   coalesce(t.version, 0) + 1                       AS version,        -- new=1, changed=prev+1
# MAGIC   current_timestamp()                              AS last_updated,
# MAGIC   false                                            AS is_deleted
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC          sha2(concat_ws('||',
# MAGIC              coalesce(first_name,''), coalesce(last_name,''),
# MAGIC              coalesce(email,''),      coalesce(city,'')), 256) AS src_hash
# MAGIC   FROM scd_type_2_catalog.scd_type_2_schema.source_customers
# MAGIC ) s
# MAGIC LEFT JOIN scd_type_2_catalog.scd_type_2_schema.customers_scd2 t
# MAGIC   ON t.customer_id = s.customer_id AND t.is_current = true
# MAGIC WHERE t.customer_id IS NULL                  -- brand new key
# MAGIC    OR t.record_hash <> s.src_hash;           -- changed attributes
# MAGIC
# MAGIC SELECT * FROM scd_type_2_catalog.scd_type_2_schema.customers_scd2 ORDER BY customer_id, version;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Expire current versions that are missing from the source (soft delete)
# MAGIC MERGE INTO scd_type_2_catalog.scd_type_2_schema.customers_scd2 AS tgt
# MAGIC USING (SELECT customer_id FROM scd_type_2_catalog.scd_type_2_schema.source_customers) AS src
# MAGIC ON  tgt.customer_id = src.customer_id
# MAGIC AND tgt.is_current  = true
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   UPDATE SET
# MAGIC     effective_to = current_timestamp(),
# MAGIC     is_current   = false,
# MAGIC     is_deleted   = true,
# MAGIC     last_updated = current_timestamp();
# MAGIC
# MAGIC -- (Optional) Hard purge after N days:
# MAGIC -- DELETE FROM scd_type_2_catalog.scd_type_2_schema.customers_scd2
# MAGIC -- WHERE is_deleted = true AND effective_to < date_sub(current_date(), 30);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulate changes in the source
# MAGIC -- 1) Update: change John’s email (customer 1)
# MAGIC UPDATE scd_type_2_catalog.scd_type_2_schema.source_customers
# MAGIC SET email = 'john.doe@new.com', updated_at = current_timestamp()
# MAGIC WHERE customer_id = 1;
# MAGIC
# MAGIC -- 2) Insert: new customer 4
# MAGIC INSERT INTO scd_type_2_catalog.scd_type_2_schema.source_customers VALUES
# MAGIC (4, 'Priya', 'Kumar', 'priya@example.com', 'Chennai', current_timestamp());
# MAGIC
# MAGIC -- 3) Delete: remove customer 2 from the source
# MAGIC DELETE FROM scd_type_2_catalog.scd_type_2_schema.source_customers
# MAGIC WHERE customer_id = 2;
# MAGIC
# MAGIC -- Re-run SCD2 Step A (expire changed currents)
# MAGIC MERGE INTO scd_type_2_catalog.scd_type_2_schema.customers_scd2 AS tgt
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     sha2(concat_ws('||',
# MAGIC       coalesce(first_name,''), coalesce(last_name,''),
# MAGIC       coalesce(email,''),      coalesce(city,'')), 256) AS src_hash
# MAGIC   FROM scd_type_2_catalog.scd_type_2_schema.source_customers
# MAGIC ) AS src
# MAGIC ON  tgt.customer_id = src.customer_id
# MAGIC AND tgt.is_current  = true
# MAGIC WHEN MATCHED AND tgt.record_hash <> src.src_hash THEN
# MAGIC   UPDATE SET
# MAGIC     effective_to = current_timestamp(),
# MAGIC     is_current   = false,
# MAGIC     last_updated = current_timestamp();
# MAGIC
# MAGIC -- Re-run SCD2 Step B (insert new versions / new keys)
# MAGIC INSERT INTO scd_type_2_catalog.scd_type_2_schema.customers_scd2
# MAGIC SELECT
# MAGIC   s.customer_id, s.first_name, s.last_name, s.email, s.city, s.updated_at,
# MAGIC   s.src_hash                                       AS record_hash,
# MAGIC   coalesce(s.updated_at, current_timestamp())      AS effective_from,
# MAGIC   TIMESTAMP '9999-12-31 23:59:59'                  AS effective_to,
# MAGIC   true                                             AS is_current,
# MAGIC   coalesce(t.version, 0) + 1                       AS version,
# MAGIC   current_timestamp()                              AS last_updated,
# MAGIC   false                                            AS is_deleted
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC          sha2(concat_ws('||',
# MAGIC              coalesce(first_name,''), coalesce(last_name,''),
# MAGIC              coalesce(email,''),      coalesce(city,'')), 256) AS src_hash
# MAGIC   FROM scd_type_2_catalog.scd_type_2_schema.source_customers
# MAGIC ) s
# MAGIC LEFT JOIN scd_type_2_catalog.scd_type_2_schema.customers_scd2 t
# MAGIC   ON t.customer_id = s.customer_id AND t.is_current = true
# MAGIC WHERE t.customer_id IS NULL
# MAGIC    OR t.record_hash <> s.src_hash;
# MAGIC
# MAGIC -- Soft-delete missing keys
# MAGIC MERGE INTO scd_type_2_catalog.scd_type_2_schema.customers_scd2 AS tgt
# MAGIC USING (SELECT customer_id FROM scd_type_2_catalog.scd_type_2_schema.source_customers) AS src
# MAGIC ON  tgt.customer_id = src.customer_id
# MAGIC AND tgt.is_current  = true
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   UPDATE SET
# MAGIC     effective_to = current_timestamp(),
# MAGIC     is_current   = false,
# MAGIC     is_deleted   = true,
# MAGIC     last_updated = current_timestamp();
# MAGIC
# MAGIC -- Inspect history
# MAGIC SELECT * FROM scd_type_2_catalog.scd_type_2_schema.customers_scd2
# MAGIC ORDER BY customer_id, version;
# MAGIC
# MAGIC -- Current view only
# MAGIC SELECT * FROM scd_type_2_catalog.scd_type_2_schema.customers_scd2
# MAGIC WHERE is_current = true
# MAGIC ORDER BY customer_id;
# MAGIC
# MAGIC -- Full timeline for a single key (e.g., 1)
# MAGIC SELECT * FROM scd_type_2_catalog.scd_type_2_schema.customers_scd2
# MAGIC WHERE customer_id = 4
# MAGIC ORDER BY version;
# MAGIC