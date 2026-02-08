# Databricks notebook source
# MAGIC %md
# MAGIC #### Module 2: Managed vs External Delta Tables
# MAGIC
# MAGIC This module explores the critical differences between managed and external Delta tables. **External tables point to S3 locations OUTSIDE Unity Catalog, NOT Volumes**.
# MAGIC
# MAGIC #### Learning Objectives
# MAGIC - Understand that external tables use external S3 storage
# MAGIC - Learn that Volumes are for managed storage, NOT external tables
# MAGIC - Explore storage and governance implications
# MAGIC - Practice creating both types correctly

# COMMAND ----------

from Includes.config import *

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

print(f'{CATALOG}')

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

# Configuration - Update for your environment
# CATALOG = "your_catalog_name"
SCHEMA = "delta_demo"
EXTERNAL_S3_BUCKET = "s3://healthcare-data-databricks"  # EXTERNAL S3 bucket outside UC
S3_PATH_PREFIX = "delta_demo"
hexa_string_suffix_gen = uuid.uuid4().hex


# Set context
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Using catalog: {CATALOG}, schema: {SCHEMA}")
print(f"🔑 External tables will use S3: s3://{EXTERNAL_S3_BUCKET}/{S3_PATH_PREFIX}/{hexa_string_suffix_gen}")
print("🚨 CRITICAL: External tables use S3 buckets OUTSIDE Unity Catalog!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Managed vs External Tables Overview
# MAGIC
# MAGIC | Aspect | Managed Tables | External Tables |
# MAGIC |--------|---------------|-----------------|
# MAGIC | **Storage** | Unity Catalog managed | **External S3 buckets** |
# MAGIC | **Location Control** | UC chooses location | **You specify S3 path** |
# MAGIC | **Lifecycle** | Data deleted with table | **Data survives table drop** |
# MAGIC | **Governance** | Full Unity Catalog integration | Limited governance |
# MAGIC | **Volumes Usage** | Can use Volumes | **NEVER uses Volumes** |
# MAGIC | **Best For** | New pipelines | **Legacy S3 data** |
# MAGIC
# MAGIC ### 🚨 **Key Concept**
# MAGIC - **External Tables** = Point to **S3 buckets outside Unity Catalog**
# MAGIC - **Volumes** = Unity Catalog managed storage (for managed tables)
# MAGIC - **NEVER** use Volume paths for external tables!

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 1: Creating Managed Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Managed Table with SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE managed_customers (
# MAGIC   customer_id BIGINT NOT NULL,
# MAGIC   first_name STRING NOT NULL,
# MAGIC   last_name STRING NOT NULL,
# MAGIC   email STRING,
# MAGIC   signup_date STRING,
# MAGIC   customer_tier STRING,
# MAGIC   lifetime_value DOUBLE,
# MAGIC   is_active BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (customer_tier)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )
# MAGIC COMMENT 'Managed table - Unity Catalog controls storage location'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managed Table with Python

# COMMAND ----------

# Sample data with simple types
managed_customers_data = [
    (1001, 'Sarah', 'Johnson', 'sarah.j@company.com', '2024-01-15', 'Gold', 5000.0, True),
    (1002, 'Michael', 'Chen', 'michael.c@company.com', '2024-01-16', 'Silver', 2500.0, True),
    (1003, 'Emily', 'Rodriguez', 'emily.r@company.com', '2024-01-17', 'Bronze', 750.0, True),
    (1004, 'David', 'Thompson', 'david.t@company.com', '2024-01-18', 'Platinum', 8500.0, True),
    (1005, 'Anna', 'Kim', 'anna.k@company.com', '2024-01-19', 'Gold', 4200.0, True)
]

managed_schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("customer_tier", StringType(), True),
    StructField("lifetime_value", DoubleType(), True),
    StructField("is_active", BooleanType(), True)
])

# Create DataFrame and save as managed table
managed_df = spark.createDataFrame(managed_customers_data, managed_schema)

(managed_df.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("customer_tier")
 .saveAsTable("managed_customers"))

print("✅ Managed table created - Unity Catalog manages storage")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 2: Creating External Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### External Table Concept - S3 Outside Unity Catalog

# COMMAND ----------

# EXTERNAL TABLE: Points to S3 bucket outside Unity Catalog

try:
  external_s3_path = f"s3://{EXTERNAL_S3_BUCKET}/{S3_PATH_PREFIX}/external_customers"
  print("🌐 EXTERNAL TABLE CONCEPT:")
  print("• Points to S3 bucket OUTSIDE Unity Catalog")
  print("• Data exists independently of Databricks")
  print("• Perfect for legacy data lakes")
  print(f"• Location: {external_s3_path}")
  print("\n❌ NOT using /Volumes/ - that would be managed storage!")

except Exception as e:
    print(f": {str(e)}")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE external_customers (
# MAGIC   customer_id BIGINT NOT NULL,
# MAGIC   first_name STRING NOT NULL,
# MAGIC   last_name STRING NOT NULL,
# MAGIC   email STRING,
# MAGIC   signup_date STRING,
# MAGIC   customer_tier STRING DEFAULT 'Bronze',
# MAGIC   lifetime_value DOUBLE DEFAULT 0.0,
# MAGIC   is_active BOOLEAN DEFAULT true
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://healthcare-data-databricks/{S3_PATH_PREFIX}/{hexa_string_suffix_gen}/'
# MAGIC PARTITIONED BY (customer_tier)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC )
# MAGIC COMMENT 'External table - data in S3 bucket outside Unity Catalog'

# COMMAND ----------

# MAGIC %md
# MAGIC ### External Table with Python - Complete Process

# COMMAND ----------

print("🚀 CREATING EXTERNAL TABLE - PROPER WORKFLOW:")
print("1. Write data directly to EXTERNAL S3 bucket")
print("2. Create table definition pointing to that S3 location")
print("3. Unity Catalog stores metadata, S3 stores data")

# Sample data for external table
external_customers_data = [
    (2001, 'Jennifer', 'Wilson', 'jen.w@external.com', '2024-01-20', 'Platinum', 9500.0, True),
    (2002, 'Robert', 'Davis', 'rob.d@external.com', '2024-01-21', 'Gold', 6200.0, True),
    (2003, 'Lisa', 'Martinez', 'lisa.m@external.com', '2024-01-22', 'Silver', 3100.0, True),
    (2004, 'James', 'Anderson', 'james.a@external.com', '2024-01-23', 'Bronze', 850.0, True),
    (2005, 'Maria', 'Garcia', 'maria.g@external.com', '2024-01-24', 'Gold', 5400.0, True)
]

external_df = spark.createDataFrame(external_customers_data, managed_schema)

# STEP 1: Write data directly to EXTERNAL S3 location
external_s3_location = f"{EXTERNAL_S3_BUCKET}/{S3_PATH_PREFIX}/external_customers_python_1909"

(external_df.write
 .format("delta")
 .mode("overwrite")
 .save(external_s3_location))

print(f"✅ Step 1: Data written to external S3: {external_s3_location}")

# STEP 2: Create external table pointing to that S3 location
spark.sql(f"""
CREATE TABLE IF NOT EXISTS external_customers_python (
  customer_id BIGINT,
  first_name STRING,
  last_name STRING,
  email STRING,
  signup_date STRING,
  customer_tier STRING,
  lifetime_value DOUBLE,
  is_active BOOLEAN
)
USING DELTA
LOCATION '{external_s3_location}'
COMMENT 'External table: UC metadata, S3 data storage'
""")

print("✅ Step 2: External table created")
print("🎯 Result: Metadata in UC, Data in external S3")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 3: Comparing Table Storage Types

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL managed_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL external_customers_python

# COMMAND ----------

# Compare storage locations and types
def compare_table_storage():
    try:
        managed_detail = spark.sql("DESCRIBE DETAIL managed_customers").collect()[0]
        external_detail = spark.sql("DESCRIBE DETAIL external_customers_python").collect()[0]
        
        print("📊 STORAGE COMPARISON")
        print("=" * 60)
        print("MANAGED TABLE:")
        print(f"  Location: {managed_detail['location']}")
        print(f"  Size: {managed_detail['sizeInBytes']:,} bytes")
        print(f"  Files: {managed_detail['numFiles']}")
        print(f"  Type: Unity Catalog Managed")
        
        print("\nEXTERNAL TABLE:")
        print(f"  Location: {external_detail['location']}")
        print(f"  Size: {external_detail['sizeInBytes']:,} bytes")
        print(f"  Files: {external_detail['numFiles']}")
        print(f"  Type: External S3 Storage")
        
        print("\n🔑 KEY DIFFERENCE:")
        print("  Managed → UC chooses and manages location")
        print("  External → Points to your specified S3 bucket")
        print("=" * 60)
    except Exception as e:
        print(f"Error comparing tables: {e}")

compare_table_storage()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 4: Lifecycle Management - Drop Table Behavior

# COMMAND ----------

# Create test tables to demonstrate drop behavior
print("Creating test tables to demonstrate lifecycle differences...")

# Test managed table
spark.sql("""
CREATE OR REPLACE TABLE test_managed_lifecycle (
  id INT,
  name STRING,
  value DOUBLE
) USING DELTA
""")

# Test external table
test_external_s3_path = f"{EXTERNAL_S3_BUCKET}/{S3_PATH_PREFIX}/test_external_lifecycle"
test_data = [(1, 'Test A', 100.5), (2, 'Test B', 200.7), (3, 'Test C', 300.9)]
test_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("value", DoubleType(), True)
])

test_df = spark.createDataFrame(test_data, test_schema)
test_df.write.format("delta").mode("overwrite").save(test_external_s3_path)

spark.sql(f"""
CREATE OR REPLACE TABLE test_external_lifecycle (
  id INT,
  name STRING,
  value DOUBLE
) USING DELTA
LOCATION '{test_external_s3_path}'
""")

print("✅ Test tables created")

# COMMAND ----------

# Get table locations before dropping
managed_location = spark.sql("DESCRIBE DETAIL test_managed_lifecycle").collect()[0]['location']
external_location = test_external_s3_path

print(f"Managed table location: {managed_location}")
print(f"External table location: {external_location}")

# Drop both tables
spark.sql("DROP TABLE test_managed_lifecycle")
spark.sql("DROP TABLE test_external_lifecycle")
print("\n✅ Both tables dropped from Unity Catalog")

# COMMAND ----------

# Check data persistence after table drop
print("🔍 Checking data persistence after DROP TABLE...")

print("\nMANAGED TABLE:")
try:
    files = dbutils.fs.ls(managed_location)
    print(f"  ❌ Unexpected: Data still exists ({len(files)} files)")
except Exception:
    print("  ✅ EXPECTED: Data DELETED - Unity Catalog removed data with table")

print("\nEXTERNAL TABLE:")
try:
    files = dbutils.fs.ls(external_location)
    print(f"  ✅ EXPECTED: Data PRESERVED in S3 ({len(files)} files)")
    print("     External S3 data survives table deletion")
except Exception:
    print("  ❌ Unexpected: External data also deleted")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 5: Best Practices and Decision Framework

# COMMAND ----------

# Decision framework for managed vs external tables
decision_factors = [
    ("New Data Pipeline", "Use Managed", "Full UC integration, optimized performance"),
    ("Legacy S3 Data Lake", "Use External", "Point to existing S3 buckets"),
    ("Cross-Workspace Sharing", "Use External", "S3 data accessible from multiple workspaces"),
    ("Strict Data Location Control", "Use External", "Specify exact S3 bucket/region"),
    ("Simple Lifecycle Management", "Use Managed", "UC handles everything automatically"),
    ("Integration with Non-Databricks Tools", "Use External", "Other systems can access S3 directly"),
    ("Regulatory Compliance", "Depends", "May require specific S3 locations (external)"),
    ("Cost Optimization", "Use Managed", "UC optimizes storage and compute together")
]

decision_df = spark.createDataFrame(
    decision_factors, 
    ["Scenario", "Recommendation", "Reasoning"]
)

print("🎯 DECISION FRAMEWORK: When to Use Each Table Type")
decision_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 6: Summary and Key Takeaways

# COMMAND ----------

print("""
🎯 KEY TAKEAWAYS: Managed vs External Tables
============================================

✅ MANAGED TABLES:
  • Unity Catalog fully manages storage location
  • Data deleted when table is dropped
  • Best for: NEW data pipelines, standard workflows
  • Full UC governance and optimization

✅ EXTERNAL TABLES:
  • Point to EXTERNAL S3 buckets (outside UC)
  • Data survives table deletion
  • Best for: LEGACY S3 data, cross-system sharing
  • You control S3 location and permissions

🚨 CRITICAL CONCEPTS:
  • External tables = S3 buckets OUTSIDE Unity Catalog
  • Volumes = UC managed storage (NOT for external tables)
  • NEVER use Volume paths for external tables!

📈 RECOMMENDATIONS:
  1. Start with MANAGED tables for new projects
  2. Use EXTERNAL only when pointing to existing S3 data
  3. Consider data governance requirements early
  4. Plan for data lifecycle and sharing needs
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleanup (Optional)