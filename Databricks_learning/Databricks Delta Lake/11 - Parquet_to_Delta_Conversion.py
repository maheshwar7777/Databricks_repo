# Databricks notebook source
# MAGIC %md
# MAGIC #### Parquet to Delta Conversion Test
# MAGIC
# MAGIC This is a minimal test case to verify successful Parquet to Delta conversion without partitioning.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from Includes.config import *
import time

# Simple configuration
SCHEMA = "test_demo"

# Set catalog and create schema
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

print(f"Using catalog: {CATALOG}")
print(f"Using schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define S3 Paths (Update These!)

# COMMAND ----------

# Update these paths with your actual S3 bucket
S3_BUCKET = "healthcare-data-databricks"  # <-- UPDATE THIS
S3_PATH = f"s3://{S3_BUCKET}/test-conversion/simple_table"

print(f"S3 Path: {S3_PATH}")
print("⚠️  Make sure to update S3_BUCKET with your actual bucket name!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cleanup Existing Tables

# COMMAND ----------

# Clean up any existing tables
try:
    spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}.simple_parquet")
    spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}.simple_delta") 
    print("✅ Existing tables cleaned up")
except Exception as e:
    print(f"Cleanup warning: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Simple Sample Data

# COMMAND ----------

# Create a simple dataset - NO partitioning
print("Creating simple test data...")

simple_df = spark.sql("""
SELECT 
  CAST(id AS INT) as id,
  CONCAT('Product_', CAST(id AS STRING)) as product_name,
  CAST((rand() * 100 + 10) AS DECIMAL(10,2)) as price,
  CASE CAST(rand() * 3 AS INT)
    WHEN 0 THEN 'Active'
    WHEN 1 THEN 'Inactive'  
    ELSE 'Pending'
  END as status,
  current_timestamp() as created_at
FROM range(1, 101)
""")

print("Sample data preview:")
simple_df.show(5)
print(f"Total records: {simple_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to S3 as Parquet (No Partitioning)

# COMMAND ----------

print(f"Writing data to S3 as Parquet: {S3_PATH}")

try:
    # Write to S3 as Parquet - NO partitioning
    simple_df.write.mode("overwrite").parquet(S3_PATH)
    print("✅ Successfully wrote Parquet data to S3")
except Exception as e:
    print(f"❌ Failed to write to S3: {e}")
    print("Check your S3 permissions and bucket name!")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create External Parquet Table

# COMMAND ----------

print("Creating external Parquet table...")

try:
    spark.sql(f"""
    CREATE TABLE {SCHEMA}.simple_parquet
    USING PARQUET
    LOCATION '{S3_PATH}'
    """)
    
    # Verify table creation
    parquet_count = spark.sql(f"SELECT COUNT(*) FROM {SCHEMA}.simple_parquet").collect()[0][0]
    print(f"✅ Parquet table created with {parquet_count} records")
    
    # Show sample data
    print("Parquet table sample:")
    spark.sql(f"SELECT * FROM {SCHEMA}.simple_parquet LIMIT 3").show()
    
except Exception as e:
    print(f"❌ Failed to create Parquet table: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Convert to Delta Using CREATE AS SELECT

# COMMAND ----------

print("Converting Parquet to Delta using CREATE AS SELECT...")

try:
    # Create Delta table from Parquet table
    conversion_start = time.time()
    
    spark.sql(f"""
    CREATE TABLE {SCHEMA}.simple_delta
    USING DELTA
    AS SELECT * FROM {SCHEMA}.simple_parquet
    """)
    
    conversion_time = time.time() - conversion_start
    print(f"✅ Delta table created in {conversion_time:.2f} seconds")
    
except Exception as e:
    print(f"❌ Conversion failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Successful Conversion

# COMMAND ----------

print("Validating conversion...")

try:
    # Get record counts
    parquet_count = spark.sql(f"SELECT COUNT(*) FROM {SCHEMA}.simple_parquet").collect()[0][0]
    delta_count = spark.sql(f"SELECT COUNT(*) FROM {SCHEMA}.simple_delta").collect()[0][0]
    
    print(f"Parquet table records: {parquet_count}")
    print(f"Delta table records:   {delta_count}")
    
    # Validate record counts match
    if parquet_count == delta_count:
        print("✅ SUCCESS: Record counts match!")
    else:
        print("❌ FAILED: Record count mismatch!")
        
    # Verify table formats
    parquet_format = spark.sql(f"DESCRIBE DETAIL {SCHEMA}.simple_parquet").select("format").collect()[0][0]
    delta_format = spark.sql(f"DESCRIBE DETAIL {SCHEMA}.simple_delta").select("format").collect()[0][0]
    
    print(f"Source format: {parquet_format}")
    print(f"Target format: {delta_format}")
    
    # Show sample from Delta table
    print("\nDelta table sample:")
    spark.sql(f"SELECT * FROM {SCHEMA}.simple_delta LIMIT 3").show()
    
    # Test a simple query on both tables
    print("Testing query performance:")
    
    # Query Parquet
    start = time.time()
    spark.sql(f"SELECT status, COUNT(*) FROM {SCHEMA}.simple_parquet GROUP BY status").collect()
    parquet_time = time.time() - start
    
    # Query Delta  
    start = time.time()
    spark.sql(f"SELECT status, COUNT(*) FROM {SCHEMA}.simple_delta GROUP BY status").collect()
    delta_time = time.time() - start
    
    print(f"Parquet query time: {parquet_time:.3f}s")
    print(f"Delta query time:   {delta_time:.3f}s")
    
except Exception as e:
    print(f"❌ Validation failed: {e}")
    raise