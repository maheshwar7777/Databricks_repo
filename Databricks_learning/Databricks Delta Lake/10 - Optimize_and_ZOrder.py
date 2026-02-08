# Databricks notebook source
# MAGIC %md
# MAGIC #### OPTIMIZE and Z-ORDER Performance Tuning
# MAGIC
# MAGIC Demonstrates Delta Lake's file compaction and data clustering features

# COMMAND ----------

from Includes.config import *
from delta.tables import *
from pyspark.sql.functions import *
import time

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")

# Create schema if not exists
spark.sql("CREATE SCHEMA IF NOT EXISTS delta_demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create Table with Many Small Files

# COMMAND ----------

# Drop table if exists and create fresh
spark.sql("DROP TABLE IF EXISTS delta_demo.sales_data")

spark.sql("""
CREATE TABLE delta_demo.sales_data (
  transaction_id BIGINT,
  customer_id BIGINT,
  product_id INT,
  quantity INT,
  amount DECIMAL(10,2),
  transaction_date DATE,
  store_location STRING,
  payment_method STRING
)
USING DELTA
PARTITIONED BY (transaction_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'false',
  'delta.autoOptimize.autoCompact' = 'false'
)
""")

print("✓ Table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Insert Data in Small Batches

# COMMAND ----------

# Simple data generation using SQL - more reliable than DataFrame operations
print("Inserting data in small batches to create many small files...")

# Insert 20 small batches to create many small files
for batch in range(20):
    # Each batch inserts just a few rows to create small files
    spark.sql(f"""
    INSERT INTO delta_demo.sales_data
    SELECT 
      {batch * 100} + id as transaction_id,
      CAST(rand() * 1000 AS BIGINT) as customer_id,
      CAST(rand() * 100 AS INT) as product_id,
      CAST(rand() * 10 + 1 AS INT) as quantity,
      CAST(rand() * 500 + 10 AS DECIMAL(10,2)) as amount,
      DATE_ADD('2024-01-01', CAST(rand() * 30 AS INT)) as transaction_date,
      CASE CAST(rand() * 4 AS INT)
        WHEN 0 THEN 'Store_A'
        WHEN 1 THEN 'Store_B'
        WHEN 2 THEN 'Store_C'
        ELSE 'Store_D'
      END as store_location,
      CASE CAST(rand() * 3 AS INT)
        WHEN 0 THEN 'Credit'
        WHEN 1 THEN 'Debit'
        ELSE 'Cash'
      END as payment_method
    FROM range(50) as t(id)
    """)
    
    if (batch + 1) % 5 == 0:
        print(f"  Batch {batch + 1}/20 completed")

print("✓ Data insertion complete - created many small files")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Check Table Status (Before Optimization)

# COMMAND ----------

# Check file statistics
detail_before = spark.sql("DESCRIBE DETAIL delta_demo.sales_data").collect()[0]

print("BEFORE OPTIMIZATION:")
print("=" * 40)
print(f"Number of files: {detail_before['numFiles']}")
print(f"Total size: {detail_before['sizeInBytes']:,} bytes")
print(f"Avg file size: {detail_before['sizeInBytes'] / detail_before['numFiles']:,.0f} bytes")

# Show sample data
print("\nSample data:")
spark.sql("SELECT * FROM delta_demo.sales_data LIMIT 5").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Run OPTIMIZE to Compact Files

# COMMAND ----------

# Run OPTIMIZE command
print("Running OPTIMIZE to compact small files...")
start_time = time.time()

spark.sql("OPTIMIZE delta_demo.sales_data")

end_time = time.time()
print(f"✓ OPTIMIZE completed in {end_time - start_time:.2f} seconds")

# COMMAND ----------

# Check results after OPTIMIZE
detail_after = spark.sql("DESCRIBE DETAIL delta_demo.sales_data").collect()[0]

print("AFTER OPTIMIZATION:")
print("=" * 40)
print(f"Number of files: {detail_after['numFiles']}")
print(f"Total size: {detail_after['sizeInBytes']:,} bytes")
print(f"Avg file size: {detail_after['sizeInBytes'] / detail_after['numFiles']:,.0f} bytes")

print(f"\n📊 RESULTS:")
print(f"Files reduced from {detail_before['numFiles']} to {detail_after['numFiles']}")
print(f"Reduction ratio: {detail_before['numFiles'] / detail_after['numFiles']:.1f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Z-ORDER for Query Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test Query Performance Before Z-ORDER

# COMMAND ----------

# Define a test query
test_query = """
SELECT 
  store_location,
  payment_method,
  COUNT(*) as transactions,
  SUM(amount) as total_sales
FROM delta_demo.sales_data 
WHERE store_location IN ('Store_A', 'Store_B')
  AND amount > 250
GROUP BY store_location, payment_method
"""

print("Running query BEFORE Z-ORDER...")
start = time.time()
spark.sql(test_query).show()
print(f"Query time: {time.time() - start:.3f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply Z-ORDER Clustering

# COMMAND ----------

# Apply Z-ORDER on columns used in WHERE clause
print("Applying Z-ORDER clustering on (store_location, amount)...")
start_time = time.time()

spark.sql("""
OPTIMIZE delta_demo.sales_data
ZORDER BY (store_location, amount)
""")

print(f"✓ Z-ORDER completed in {time.time() - start_time:.2f} seconds")

# COMMAND ----------

# Test same query after Z-ORDER
# spark.sql("CLEAR CACHE")

print("Running query AFTER Z-ORDER...")
start = time.time()
spark.sql(test_query).show()
print(f"Query time: {time.time() - start:.3f} seconds")
print("\n💡 Z-ORDER co-locates related data, improving query performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Check Optimization History

# COMMAND ----------

# View optimization history
print("OPTIMIZATION HISTORY:")
print("=" * 40)

spark.sql("""
SELECT 
  version,
  operation,
  timestamp,
  operationMetrics.numRemovedFiles,
  operationMetrics.numAddedFiles
FROM (DESCRIBE HISTORY delta_demo.sales_data)
WHERE operation IN ('OPTIMIZE')
ORDER BY version DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Quick Optimization Check Function

# COMMAND ----------

def check_table_optimization(table_name):
    """
    Quick function to check if a table needs optimization
    """
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    
    num_files = detail['numFiles']
    total_size = detail['sizeInBytes']
    avg_size_mb = (total_size / num_files / 1024 / 1024) if num_files > 0 else 0
    
    print(f"Table: {table_name}")
    print(f"  Files: {num_files}")
    print(f"  Avg size: {avg_size_mb:.1f} MB")
    
    if num_files > 50:
        print("  ⚠️ Recommendation: Too many files - run OPTIMIZE")
    elif avg_size_mb < 128:
        print("  ⚠️ Recommendation: Small files - run OPTIMIZE")
    else:
        print("  ✅ Table is well optimized")
    
    return num_files, avg_size_mb

# Test the function
check_table_optimization("delta_demo.sales_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Auto-Optimize for New Tables

# COMMAND ----------

# Create table with auto-optimization enabled
spark.sql("DROP TABLE IF EXISTS delta_demo.auto_optimized_sales")

spark.sql("""
CREATE TABLE delta_demo.auto_optimized_sales (
  id BIGINT,
  amount DECIMAL(10,2),
  date DATE
)
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

# Insert data - will be automatically optimized
spark.sql("""
INSERT INTO delta_demo.auto_optimized_sales
SELECT 
  id,
  CAST(rand() * 1000 AS DECIMAL(10,2)) as amount,
  current_date() as date
FROM range(1000) as t(id)
""")

# Check - should have fewer, larger files
detail_auto = spark.sql("DESCRIBE DETAIL delta_demo.auto_optimized_sales").collect()[0]
print(f"Auto-optimized table: {detail_auto['numFiles']} files")
print("✓ Auto-optimization keeps file count optimal automatically")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Key Commands:
# MAGIC
# MAGIC 1. **OPTIMIZE** - Compacts small files into larger ones
# MAGIC    ```sql
# MAGIC    OPTIMIZE table_name
# MAGIC    ```
# MAGIC
# MAGIC 2. **Z-ORDER** - Clusters data by multiple columns
# MAGIC    ```sql
# MAGIC    OPTIMIZE table_name ZORDER BY (col1, col2)
# MAGIC    ```
# MAGIC
# MAGIC 3. **Auto-Optimize** - Enable automatic optimization
# MAGIC    ```sql
# MAGIC    TBLPROPERTIES (
# MAGIC      'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC      'delta.autoOptimize.autoCompact' = 'true'
# MAGIC    )
# MAGIC    ```
# MAGIC
# MAGIC ### When to Use:
# MAGIC - **OPTIMIZE**: When you have > 50 files or files < 128MB
# MAGIC - **Z-ORDER**: For tables with multi-column filter queries
# MAGIC - **Auto-Optimize**: For streaming or frequently updated tables