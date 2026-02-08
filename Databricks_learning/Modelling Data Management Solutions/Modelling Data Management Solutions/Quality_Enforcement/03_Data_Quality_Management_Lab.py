# Databricks notebook source
# MAGIC %md
# MAGIC #Data Quality Management Lab in Databricks
# MAGIC
# MAGIC ### Overview
# MAGIC This notebook demonstrates how to apply **Data Quality (DQ) principles** within the  
# MAGIC **Medallion Architecture (Bronze → Silver → Gold)** using Databricks features:
# MAGIC
# MAGIC - Ingest raw JSON data into the Bronze layer  
# MAGIC - Apply **quality rules** (null checks, enums, type validation) in the Silver layer  
# MAGIC - Separate **good** vs **quarantine** rows for audit and remediation  
# MAGIC - Enforce **Delta constraints** and calculate basic **DQ metrics**  
# MAGIC - Use **rescuedDataColumn** to capture schema drift and unexpected fields  
# MAGIC
# MAGIC 📌 By the end of this lab, you will know how to:  
# MAGIC 1. Ingest raw data using Batch   
# MAGIC 2. Apply validation rules to filter and quarantine bad data  
# MAGIC 3. Monitor quality metrics to ensure downstream trust  
# MAGIC 4. Reset and re-run pipelines for repeatable demos  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Management — Hands-on Tutorial
# MAGIC
# MAGIC This mini-lab shows:
# MAGIC 1) **Bronze**: Ingest raw JSON with Auto Loader  
# MAGIC 2) **Silver**: Apply DQ rules → split **good** vs **quarantine**  
# MAGIC 3) **Constraints & Metrics**: Enforce + measure quality  
# MAGIC 4) **Reset**: Clean up to re-run anytime
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- %sql
# MAGIC -- Setup (idempotent)
# MAGIC CREATE CATALOG IF NOT EXISTS streaming_events_06;
# MAGIC USE CATALOG streaming_events_06;
# MAGIC
# MAGIC CREATE SCHEMA  IF NOT EXISTS bronze_demo;
# MAGIC USE SCHEMA bronze_demo;
# MAGIC
# MAGIC CREATE VOLUME  IF NOT EXISTS _dq_lab
# MAGIC COMMENT 'DQ lab: source + tables';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Common Config (used by all cells)
# MAGIC
# MAGIC
# MAGIC
# MAGIC We store and read files under a UC Volume path /Volumes/....
# MAGIC
# MAGIC Batch ingest keeps this lab portable across compute types.

# COMMAND ----------

# 1) Common config (do once)
import time, json
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType

catalog = "streaming_events_06"
schema  = "bronze_demo"
volume  = "_dq_lab"
VOL_BASE = f"/Volumes/{catalog}/{schema}/{volume}"

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE `{catalog}`.`{schema}`")

# Explicit schema for Bronze rows
bronze_schema = StructType([
    StructField("user_id",    IntegerType(), True),
    StructField("event_type", StringType(),  True),
    StructField("ts",         LongType(),    True),
])

def new_src_folder() -> str:
    """Create a fresh source folder so every run is clean."""
    rid = str(int(time.time()*1000))
    src = f"{VOL_BASE}/dq/src_{rid}"
    dbutils.fs.mkdirs(src)
    return src


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC **2) Bronze — Seed Files & Ingest (Batch)**
# MAGIC
# MAGIC We write two small JSON files into the Volume.
# MAGIC
# MAGIC We batch-read them and save as a Delta Bronze table.

# COMMAND ----------

# 2) Bronze ingest (batch)
src = new_src_folder()
tbl_bronze = f"{catalog}.{schema}.dq_bronze"

# Write two tiny NDJSON files (mix good + a bad row for demo)
file_1 = f"{src}/events_01.json"
file_2 = f"{src}/events_02.json"
dbutils.fs.put(
    file_1,
    json.dumps({"user_id":101, "event_type":"open",  "ts":int(time.time())}) + "\n" +
    json.dumps({"user_id":102, "event_type":"close", "ts":int(time.time())+1}) + "\n",
    overwrite=True
)
dbutils.fs.put(
    file_2,
    json.dumps({"user_id":103, "event_type":"click", "ts":int(time.time())+2}) + "\n" +
    json.dumps({"user_id":None, "event_type":"oops",  "ts":int(time.time())+3}) + "\n",
    overwrite=True
)

print("📂 Source folder:", src)
display(dbutils.fs.ls(src))

# Create/overwrite Bronze as Delta (BATCH)
spark.sql(f"DROP TABLE IF EXISTS {tbl_bronze}")
(spark.read
      .schema(bronze_schema)
      .json(src)
      .write
      .mode("overwrite")
      .format("delta")
      .saveAsTable(tbl_bronze))

display(spark.sql(f"SELECT COUNT(*) AS rows FROM {tbl_bronze}"))
display(spark.sql(f"SELECT * FROM {tbl_bronze} ORDER BY ts"))


# COMMAND ----------

# 2) Bronze ingest (batch) — with intentional duplicates
src = new_src_folder()
tbl_bronze = f"{catalog}.{schema}.dq_bronze"

# Write two tiny NDJSON files (mix good + a bad row for lab demo)
file_1 = f"{src}/events_01.json"
file_2 = f"{src}/events_02.json"
dbutils.fs.put(
    file_1,
    json.dumps({"user_id":101, "event_type":"open",  "ts":int(time.time())}) + "\n" +
    json.dumps({"user_id":102, "event_type":"close", "ts":int(time.time())+1}) + "\n",
    overwrite=True
)
dbutils.fs.put(
    file_2,
    json.dumps({"user_id":103, "event_type":"click", "ts":int(time.time())+2}) + "\n" +
    json.dumps({"user_id":None, "event_type":"oops",  "ts":int(time.time())+3}) + "\n",
    overwrite=True
)

# ✅ Add a third file that DUPLICATES some (user_id, ts) combos from above
# We'll duplicate (101, ts0) and (102, ts0+1) to create exact dupes.
ts0 = int(time.time())  # not used to write new, just to make another pair too
first_rows = spark.read.schema(bronze_schema).json(src).orderBy("ts").limit(2).collect()
dupe_payload = ""
for r in first_rows:
    dupe_payload += json.dumps({"user_id": int(r["user_id"]) if r["user_id"] is not None else None,
                                "event_type": r["event_type"],
                                "ts": int(r["ts"])}) + "\n"

# also add one more explicit duplicate pair to be extra clear
dupe_payload += json.dumps({"user_id": 200, "event_type":"open",  "ts": 999999001}) + "\n"
dupe_payload += json.dumps({"user_id": 200, "event_type":"open",  "ts": 999999001}) + "\n"

file_3 = f"{src}/events_dupes.json"
dbutils.fs.put(file_3, dupe_payload, overwrite=True)

print("📂 Source folder:", src)
display(dbutils.fs.ls(src))

# Create/overwrite Bronze as Delta (BATCH)
spark.sql(f"DROP TABLE IF EXISTS {tbl_bronze}")
(spark.read
      .schema(bronze_schema)
      .json(src)
      .write
      .mode("overwrite")
      .format("delta")
      .saveAsTable(tbl_bronze))

# Verify: you should now see some duplicate (user_id, ts) pairs in Bronze
display(spark.sql(f"SELECT COUNT(*) AS rows FROM {tbl_bronze}"))

# Show any duplicates currently present (for your demo)
display(spark.sql(f"""
SELECT user_id, ts, COUNT(*) AS cnt
FROM {tbl_bronze}
GROUP BY user_id, ts
HAVING COUNT(*) > 1
ORDER BY cnt DESC, user_id, ts
"""))

display(spark.sql(f"SELECT * FROM {tbl_bronze} ORDER BY ts"))


# COMMAND ----------

# MAGIC %md
# MAGIC **3) Bronze+ — Metadata Management**
# MAGIC
# MAGIC Add ingest_time and source_file to help with lineage/auditing.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

bronze_with_meta = (
    spark.read.table(tbl_bronze)
         .withColumn("ingest_time", current_timestamp())
         .withColumn("source_file", col("_metadata.file_path"))  
)

tbl_bronze_meta = f"{catalog}.{schema}.dq_bronze_meta"
bronze_with_meta.write.mode("overwrite").format("delta").saveAsTable(tbl_bronze_meta)

display(spark.sql(f"SELECT * FROM {tbl_bronze_meta} ORDER BY ts"))


# COMMAND ----------

# MAGIC %md
# MAGIC **4) Silver: Apply Data Quality Rules
# MAGIC We’ll define simple validation rules:**
# MAGIC
# MAGIC user_id IS NOT NULL
# MAGIC event_type IN ('open','close','click')
# MAGIC ts must be numeric (castable to BIGINT)
# MAGIC
# MAGIC Then we’ll split into:
# MAGIC
# MAGIC dq_silver_good → clean rows that pass all rules
# MAGIC dq_silver_quarantine → rows that fail any rule

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4) Silver split
# MAGIC USE CATALOG streaming_events_06;
# MAGIC USE SCHEMA  bronze_demo;
# MAGIC
# MAGIC DROP TABLE IF EXISTS dq_silver_good;
# MAGIC DROP TABLE IF EXISTS dq_silver_quarantine;
# MAGIC
# MAGIC CREATE TABLE dq_silver_good AS
# MAGIC SELECT
# MAGIC   user_id,
# MAGIC   lower(event_type) AS event_type,
# MAGIC   CAST(ts AS BIGINT) AS ts
# MAGIC FROM dq_bronze
# MAGIC WHERE user_id IS NOT NULL
# MAGIC   AND lower(event_type) IN ('open','close','click')
# MAGIC   AND ts RLIKE '^[0-9]+$';
# MAGIC
# MAGIC CREATE TABLE dq_silver_quarantine AS
# MAGIC SELECT *
# MAGIC FROM dq_bronze
# MAGIC WHERE NOT (
# MAGIC   user_id IS NOT NULL
# MAGIC   AND lower(event_type) IN ('open','close','click')
# MAGIC   AND ts RLIKE '^[0-9]+$'
# MAGIC );
# MAGIC
# MAGIC -- quick counts
# MAGIC SELECT 'good' AS bucket, COUNT(*) AS rows FROM dq_silver_good
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine', COUNT(*) FROM dq_silver_quarantine;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC **5) Schema Enforcement (Silver)**
# MAGIC
# MAGIC Add constraints to enforce expected values and prevent bad writes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5) Schema enforcement via constraints
# MAGIC ALTER TABLE dq_silver_good DROP CONSTRAINT IF EXISTS c_user_not_null;
# MAGIC ALTER TABLE dq_silver_good ADD CONSTRAINT c_user_not_null
# MAGIC CHECK (user_id IS NOT NULL);
# MAGIC
# MAGIC ALTER TABLE dq_silver_good DROP CONSTRAINT IF EXISTS c_event_type_allowed;
# MAGIC ALTER TABLE dq_silver_good ADD CONSTRAINT c_event_type_allowed
# MAGIC CHECK (event_type IN ('open','close','click'));
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Deduplication & Uniqueness
# MAGIC
# MAGIC Duplicate events can corrupt KPIs and downstream models.  
# MAGIC We ensure uniqueness by applying `ROW_NUMBER()` or Delta **MERGE** into a deduped Silver table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Are there any duplicates in the *non-deduped* Silver table?
# MAGIC SELECT user_id, ts, COUNT(*) AS cnt
# MAGIC FROM dq_silver_good
# MAGIC GROUP BY user_id, ts
# MAGIC HAVING COUNT(*) > 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Duplicates present BEFORE dedup?
# MAGIC SELECT user_id, ts, COUNT(*) AS cnt
# MAGIC FROM dq_silver_good
# MAGIC GROUP BY user_id, ts
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY cnt DESC, user_id, ts;
# MAGIC

# COMMAND ----------

# 6b) DataFrame dropDuplicates approach (simple and effective)
tbl_dedup2 = f"{catalog}.{schema}.dq_silver_deduped_df"

deduped_df = (
    spark.read.table(f"{catalog}.{schema}.dq_silver_good")
         .dropDuplicates(["user_id", "ts"])
)

deduped_df.write.mode("overwrite").format("delta").saveAsTable(tbl_dedup2)

display(spark.sql(f"SELECT * FROM {tbl_dedup2} ORDER BY ts"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## -- Duplicates AFTER dedup (should be none)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Duplicates AFTER dedup (should be none)
# MAGIC SELECT user_id, ts, COUNT(*) AS cnt
# MAGIC FROM dq_silver_deduped_sql
# MAGIC GROUP BY user_id, ts
# MAGIC HAVING COUNT(*) > 1;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quantify how many rows were removed by dedup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many were removed by dedup?
# MAGIC WITH counts AS (
# MAGIC   SELECT
# MAGIC     (SELECT COUNT(*) FROM dq_silver_good)                  AS total_rows,
# MAGIC     (SELECT COUNT(*) FROM dq_silver_deduped_sql)           AS dedup_rows,
# MAGIC     (SELECT COUNT(DISTINCT user_id, ts) FROM dq_silver_good) AS distinct_pairs
# MAGIC )
# MAGIC SELECT
# MAGIC   total_rows,
# MAGIC   dedup_rows,
# MAGIC   (total_rows - dedup_rows) AS rows_removed,
# MAGIC   distinct_pairs FROM counts;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC **7) Flagging Violations (inline alternative to quarantine)**
# MAGIC
# MAGIC Keep all rows, but mark validity with a dq_status flag.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7) Flag violations
# MAGIC DROP TABLE IF EXISTS dq_silver_flagged;
# MAGIC
# MAGIC CREATE TABLE dq_silver_flagged AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE 
# MAGIC     WHEN user_id IS NOT NULL
# MAGIC      AND lower(event_type) IN ('open','close','click')
# MAGIC      AND ts RLIKE '^[0-9]+$'
# MAGIC     THEN 'PASS'
# MAGIC     ELSE 'FAIL'
# MAGIC   END AS dq_status
# MAGIC FROM dq_bronze;
# MAGIC
# MAGIC SELECT dq_status, COUNT(*) AS rows
# MAGIC FROM dq_silver_flagged
# MAGIC GROUP BY dq_status;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8) Data Quality Metrics
# MAGIC
# MAGIC We can compute pass/fail rates for each rule on the **Bronze** table.

# COMMAND ----------

# DBTITLE 1,Data Quality Scorecard
# MAGIC %sql
# MAGIC -- 8) DQ metrics
# MAGIC WITH rules AS (
# MAGIC   SELECT
# MAGIC     CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END AS r_user_not_null,
# MAGIC     CASE WHEN lower(event_type) IN ('open','close','click') THEN 1 ELSE 0 END AS r_event_in_set,
# MAGIC     CASE WHEN CAST(ts AS BIGINT) IS NOT NULL THEN 1 ELSE 0 END AS r_ts_numeric
# MAGIC   FROM dq_bronze
# MAGIC )
# MAGIC SELECT
# MAGIC   ROUND(AVG(r_user_not_null)*100,1) AS pct_user_not_null,
# MAGIC   ROUND(AVG(r_event_in_set)*100,1)  AS pct_event_in_set,
# MAGIC   ROUND(AVG(r_ts_numeric)*100,1)    AS pct_ts_numeric
# MAGIC FROM rules;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9) Rollback & Time Travel with Delta
# MAGIC
# MAGIC Databricks Delta tables support **time travel**.  
# MAGIC We can query old versions or restore data after accidental writes.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 9) Time travel
# MAGIC DESCRIBE HISTORY dq_silver_good;
# MAGIC
# MAGIC -- peek an older version (adjust 0/1/2 as needed)
# MAGIC -- SELECT * FROM dq_silver_good VERSION AS OF 0;
# MAGIC
# MAGIC -- restore example (uncomment to execute)
# MAGIC -- RESTORE TABLE dq_silver_good TO VERSION AS OF 0;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10) Add Constraints to Enforce Quality
# MAGIC
# MAGIC Use Delta constraints to prevent invalid data from entering Silver.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- %sql
# MAGIC ALTER TABLE dq_silver_good DROP CONSTRAINT IF EXISTS c_event_type_allowed;
# MAGIC
# MAGIC ALTER TABLE dq_silver_good ADD CONSTRAINT c_event_type_allowed
# MAGIC CHECK (event_type IN ('open','close','click'));
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11) Reset Lab State (safe to re-run)
# MAGIC
# MAGIC Drop all lab tables and clear state in the Volume so you can restart fresh.
# MAGIC

# COMMAND ----------

# Reset cell (safe to re-run the whole lab)
tables = [
  "dq_bronze",
  "dq_silver_good",
  "dq_silver_quarantine",
  "dq_silver_flagged",
  "dq_silver_deduped"
]
for t in tables:
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{t}")

print("✅ Reset complete (tables dropped). Rerun the Bronze cell to reload demo data.")