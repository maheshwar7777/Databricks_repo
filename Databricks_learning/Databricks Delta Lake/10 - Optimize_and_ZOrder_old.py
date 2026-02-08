# Databricks notebook source
# MAGIC %md
# MAGIC #### Module 6: OPTIMIZE and Z-ORDER Performance Tuning
# MAGIC
# MAGIC This module covers Delta Lake's performance optimization features, including file compaction with OPTIMIZE and multi-dimensional clustering with Z-ORDER.
# MAGIC
# MAGIC #### Learning Objectives
# MAGIC - Understand the small file problem and its impact
# MAGIC - Learn when and how to use OPTIMIZE for file compaction
# MAGIC - Master Z-ORDER for multi-dimensional clustering
# MAGIC - Implement automated optimization strategies
# MAGIC - Monitor and measure optimization effectiveness

# COMMAND ----------

from Includes.config import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Understanding the Small File Problem
# MAGIC
# MAGIC #### Why Small Files Hurt Performance
# MAGIC
# MAGIC | Issue | Impact |
# MAGIC |-------|--------|
# MAGIC | **List Operations** | More files = slower metadata operations |
# MAGIC | **Query Planning** | Overhead per file during planning |
# MAGIC | **Task Scheduling** | Too many small tasks reduce parallelism efficiency |
# MAGIC | **Memory Overhead** | Each file requires metadata in driver memory |
# MAGIC | **Cloud Storage Costs** | More API calls = higher costs |

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 1: Creating Tables with Small Files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Generate Table with Many Small Files

# COMMAND ----------

# Create a table that will demonstrate the small file problem
spark.sql("""
CREATE OR REPLACE TABLE delta_demo.sales_data (
  transaction_id BIGINT,
  customer_id BIGINT,
  product_id BIGINT,
  quantity INT,
  unit_price DECIMAL(10,2),
  total_amount DECIMAL(12,2),
  transaction_date DATE,
  store_location STRING,
  payment_method STRING,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (transaction_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'false',  -- Disable auto-optimize to see small files
  'delta.autoOptimize.autoCompact' = 'false',
  'delta.feature.allowColumnDefaults' = 'supported'
)
""")

print("Sales data table created with auto-optimization disabled")

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql.functions import col, to_timestamp
 
def generate_sales_batch(batch_id, num_records=50):
    start_date = datetime(2024, 1, 1)
    sales_data = []
    for i in range(num_records):
        transaction_date = start_date + timedelta(days=random.randint(0, 30))
 
        transaction_id = int(batch_id * 1000 + i)        # transaction_id
        customer_id    = int(random.randint(1, 1000))    # customer_id
        product_id     = int(random.randint(1, 100))     # product_id
        quantity       = int(random.randint(1, 10))      # quantity
        unit_price     = float(round(random.uniform(10.0, 500.0), 2))  # unit_price
        total_amount   = round(quantity * unit_price, 2) # total_amount
 
        # convert datetime to ISO string to avoid mixed-type inference
        transaction_date_str = transaction_date.isoformat()
 
        store_location = random.choice(['Store_A', 'Store_B', 'Store_C', 'Store_D'])
        payment_method = random.choice(['Credit', 'Debit', 'Cash', 'Mobile'])
 
        # IMPORTANT: append a single tuple (row) — not multiple args
        sales_data.append((
            transaction_id,
            customer_id,
            product_id,
            quantity,
            unit_price,
            total_amount,
            transaction_date_str,
            store_location,
            payment_method
        ))
 
    # Defensive: ensure we return a list (not None) and rows are homogeneous
    if not isinstance(sales_data, list):
        raise AssertionError("generate_sales_batch must return a list, got: " + str(type(sales_data)))
    return sales_data
 
 
print("Inserting data in small batches to create small files...")
 
for batch in range(20):
    # generate data
    batch_data = generate_sales_batch(batch, 50)
 
    # Defensive checks BEFORE calling spark.createDataFrame
    if batch_data is None:
        raise AssertionError(f"generate_sales_batch returned None for batch {batch}")
    if not isinstance(batch_data, list):
        raise AssertionError(f"generate_sales_batch must return a list; got {type(batch_data)} for batch {batch}")
    if len(batch_data) == 0:
        raise AssertionError(f"generate_sales_batch returned empty list for batch {batch}")
 
    # check that all rows have the same length (should be 9 items)
    expected_len = 9
    for idx, row in enumerate(batch_data):
        if not hasattr(row, '__len__'):
            raise AssertionError(f"Row {idx} in batch {batch} is not a sequence: {row}")
        if len(row) != expected_len:
            raise AssertionError(f"Row {idx} in batch {batch} has length {len(row)} but expected {expected_len}. Row: {row}")
 
    # show a sample row to console so you can visually inspect it
    print(f"Batch {batch}: sample row[0]: {batch_data[0]}")
 
    # column order must match the tuple order above
    batch_df = spark.createDataFrame(
        batch_data,
        [
            "transaction_id", "customer_id", "product_id", "quantity",
            "unit_price", "total_amount", "transaction_date",
            "store_location", "payment_method"
        ]
    )
 
    # If you want transaction_date as a timestamp column in Spark, convert explicitly:
    batch_df = batch_df.withColumn("transaction_date", to_timestamp(col("transaction_date")))
 
    write_mode = "overwrite" if batch == 0 else "append"
    (batch_df.write.format("delta").mode(write_mode).saveAsTable("delta_demo.sales_data"))
 
    if (batch + 1) % 5 == 0:
        print(f"Inserted batch {batch + 1}/20")
 
print("Data insertion complete - many small files created")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 2: Analyzing Table File Structure

# COMMAND ----------

# MAGIC %md
# MAGIC #### Before Optimization Analysis

# COMMAND ----------

# Get table details before optimization
detail_before = spark.sql("DESCRIBE DETAIL delta_demo.sales_data").collect()[0]

print("Table Status BEFORE Optimization:")
print("=" * 50)
print(f"Number of files: {detail_before['numFiles']}")
print(f"Total size (bytes): {detail_before['sizeInBytes']:,}")
print(f"Average file size: {detail_before['sizeInBytes'] / detail_before['numFiles']:,.0f} bytes")
print(f"Location: {detail_before['location']}")

# COMMAND ----------

# Analyze file distribution by partition
def analyze_file_distribution():
    try:
        # Read transaction log to see file distribution
        table_location = detail_before['location']
        log_location = f"{table_location}/_delta_log"
        
        log_df = spark.read.json(f"{log_location}/*.json")
        
        # Get add actions (files)
        add_df = log_df.select("add.*").filter(col("path").isNotNull())
        
        if add_df.count() > 0:
            print("\nFile Size Distribution:")
            print("-" * 30)
            add_df.select(
                "path",
                "size", 
                col("partitionValues").getItem("transaction_date").alias("partition_date")
            ).orderBy("size").show(20, truncate=False)
            
            # Summary statistics
            file_stats = add_df.agg(
                count("*").alias("file_count"),
                min("size").alias("min_size"),
                max("size").alias("max_size"),
                avg("size").alias("avg_size"),
                sum("size").alias("total_size")
            ).collect()[0]
            
            print(f"\nFile Statistics:")
            print(f"Total files: {file_stats['file_count']}")
            print(f"Min size: {file_stats['min_size']:,} bytes")
            print(f"Max size: {file_stats['max_size']:,} bytes")
            print(f"Avg size: {file_stats['avg_size']:,.0f} bytes")
            print(f"Total size: {file_stats['total_size']:,} bytes")
            
        else:
            print("No file information available in transaction log")
            
    except Exception as e:
        print(f"Error analyzing file distribution: {e}")

analyze_file_distribution()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 3: OPTIMIZE Command

# COMMAND ----------

# MAGIC %md
# MAGIC #### Basic OPTIMIZE Operation

# COMMAND ----------

# Measure optimization time
start_time = time.time()

# Run OPTIMIZE to compact small files
spark.sql("OPTIMIZE delta_demo.sales_data")

end_time = time.time()
print(f"OPTIMIZE completed in {end_time - start_time:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC #### After Optimization Analysis

# COMMAND ----------

# Get table details after optimization
detail_after = spark.sql("DESCRIBE DETAIL delta_demo.sales_data").collect()[0]

print("Table Status AFTER Optimization:")
print("=" * 50)
print(f"Number of files: {detail_after['numFiles']}")
print(f"Total size (bytes): {detail_after['sizeInBytes']:,}")
print(f"Average file size: {detail_after['sizeInBytes'] / detail_after['numFiles']:,.0f} bytes")

print("\nOptimization Results:")
print("-" * 30)
print(f"File reduction: {detail_before['numFiles']} → {detail_after['numFiles']}")
print(f"Reduction ratio: {detail_before['numFiles'] / detail_after['numFiles']:.1f}x")
print(f"New avg file size: {detail_after['sizeInBytes'] / detail_after['numFiles']:,.0f} bytes")

# COMMAND ----------

# Check optimization history
spark.sql("DESCRIBE HISTORY delta_demo.sales_data").select(
    "version", "operation", "operationParameters", "operationMetrics"
).filter(col("operation") == "OPTIMIZE").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 4: Z-ORDER Clustering
# MAGIC
# MAGIC
# MAGIC #### Understanding Z-ORDER
# MAGIC
# MAGIC Z-ORDER is a multi-dimensional clustering technique that:
# MAGIC - **Co-locates** related data within files
# MAGIC - **Improves data skipping** for multiple column filters
# MAGIC - **Reduces I/O** by reading fewer files for complex queries

# COMMAND ----------

# MAGIC %md
# MAGIC #### Demonstrate Query Performance Before Z-ORDER

# COMMAND ----------

# Create a query that filters on multiple columns
test_query = """
SELECT 
  store_location,
  payment_method,
  COUNT(*) as transaction_count,
  SUM(total_amount) as total_sales,
  AVG(total_amount) as avg_transaction
FROM delta_demo.sales_data 
WHERE transaction_date BETWEEN '2024-01-10' AND '2024-01-20'
  AND store_location IN ('Store_A', 'Store_B')
  AND unit_price > 100
GROUP BY store_location, payment_method
ORDER BY total_sales DESC
"""

# Measure query performance before Z-ORDER
print("Query Performance BEFORE Z-ORDER:")
start_time = time.time()
result_before = spark.sql(test_query)
result_before.show()
end_time = time.time()
print(f"Query time: {end_time - start_time:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply Z-ORDER Optimization

# COMMAND ----------

# Apply Z-ORDER on frequently queried columns
start_time = time.time()

spark.sql("""
OPTIMIZE delta_demo.sales_data
ZORDER BY (store_location, payment_method, unit_price)
""")

end_time = time.time()
print(f"Z-ORDER optimization completed in {end_time - start_time:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test Query Performance After Z-ORDER

# COMMAND ----------

# Measure query performance after Z-ORDER
print("Query Performance AFTER Z-ORDER:")
start_time = time.time()
result_after = spark.sql(test_query)
result_after.show()
end_time = time.time()
print(f"Query time: {end_time - start_time:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 5: Advanced Optimization Strategies

# COMMAND ----------

# MAGIC %md
# MAGIC #### Partition-Level Optimization

# COMMAND ----------

# Optimize specific partitions only
spark.sql("""
OPTIMIZE delta_demo.sales_data
WHERE transaction_date = '2024-01-15'
""")

print("Optimized single partition for 2024-01-15")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conditional Optimization with Python

# COMMAND ----------

def conditional_optimize_table(table_name, file_threshold=10, size_threshold=128*1024*1024):
    """
    Conditionally optimize table based on file count and size thresholds
    """
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    
    should_optimize = False
    reasons = []
    
    # Check file count threshold
    if detail['numFiles'] > file_threshold:
        should_optimize = True
        reasons.append(f"Too many files ({detail['numFiles']} > {file_threshold})")
    
    # Check average file size threshold
    avg_size = detail['sizeInBytes'] / detail['numFiles'] if detail['numFiles'] > 0 else 0
    if avg_size < size_threshold:
        should_optimize = True
        reasons.append(f"Small average file size ({avg_size:,.0f} < {size_threshold:,} bytes)")
    
    if should_optimize:
        print(f"Optimization needed for {table_name}:")
        for reason in reasons:
            print(f"  - {reason}")
        
        # Perform optimization
        start_time = time.time()
        spark.sql(f"OPTIMIZE {table_name}")
        end_time = time.time()
        
        print(f"Optimization completed in {end_time - start_time:.2f} seconds")
        
        # Show results
        detail_after = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        print(f"Files reduced: {detail['numFiles']} → {detail_after['numFiles']}")
        
    else:
        print(f"No optimization needed for {table_name}")

# Test conditional optimization
conditional_optimize_table("delta_demo.sales_data", file_threshold=5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 6: Automated Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC #### Enable Auto-Optimize Features

# COMMAND ----------

# Enable auto-optimize features for new table
spark.sql("""
CREATE OR REPLACE TABLE delta_demo.auto_optimized_sales (
  transaction_id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(10,2),
  transaction_date DATE,
  category STRING
)
USING DELTA
PARTITIONED BY (transaction_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

print("Table created with auto-optimization enabled")

# COMMAND ----------

# Insert data to test auto-optimization
for i in range(10):
    spark.sql(f"""
    INSERT INTO delta_demo.auto_optimized_sales VALUES
    ({i*100 + 1}, {i*10 + 1}, {100.0 + i*10}, '2024-01-{15 + (i % 15):02d}', 'Category_{i % 3}')
    """)

# Check if auto-optimization occurred
detail_auto = spark.sql("DESCRIBE DETAIL delta_demo.auto_optimized_sales").collect()[0]
print(f"Auto-optimized table has {detail_auto['numFiles']} files")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 7: Optimization Monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ### Track Optimization History

# COMMAND ----------

def analyze_optimization_history(table_name):
    """Analyze optimization operations from table history"""
    
    history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
    
    # Filter optimization operations
    opt_history = history_df.filter(
        col("operation").isin(["OPTIMIZE", "AUTO COMPACTION"])
    ).select(
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "operationMetrics"
    )
    
    print(f"Optimization History for {table_name}:")
    print("=" * 60)
    
    if opt_history.count() > 0:
        opt_history.show(truncate=False)
        
        # Extract metrics
        metrics_df = opt_history.select(
            "version",
            "operation",
            col("operationMetrics.numRemovedFiles").alias("files_removed"),
            col("operationMetrics.numAddedFiles").alias("files_added"),
            col("operationMetrics.filesAdded.min").alias("min_file_size"),
            col("operationMetrics.filesAdded.max").alias("max_file_size"),
            col("operationMetrics.filesAdded.avg").alias("avg_file_size")
        )
        
        print("\nOptimization Metrics:")
        metrics_df.show()
        
    else:
        print("No optimization operations found")

analyze_optimization_history("delta_demo.sales_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Monitoring Function

# COMMAND ----------

def create_optimization_dashboard(table_name):
    """Create optimization dashboard for a table"""
    
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    history = spark.sql(f"DESCRIBE HISTORY {table_name}").collect()
    
    print(f"OPTIMIZATION DASHBOARD: {table_name}")
    print("=" * 60)
    
    # Current state
    print("CURRENT STATE:")
    print(f"  Files: {detail['numFiles']}")
    print(f"  Size: {detail['sizeInBytes']:,} bytes")
    print(f"  Avg file size: {detail['sizeInBytes'] / detail['numFiles']:,.0f} bytes")
    print(f"  Partitions: {detail.get('numPartitions', 'N/A')}")
    
    # Optimization recommendations
    print("\nRECOMMENDAT​IONS:")
    avg_file_size = detail['sizeInBytes'] / detail['numFiles']
    
    if detail['numFiles'] > 50:
        print("  ⚠️  Consider OPTIMIZE - high file count")
    
    if avg_file_size < 64 * 1024 * 1024:  # 64MB
        print("  ⚠️  Consider OPTIMIZE - small average file size")
    
    if detail['numFiles'] < 10 and avg_file_size > 1024 * 1024 * 1024:  # 1GB
        print("  ✅ File size looks optimal")
    
    # Recent operations
    print("\nRECENT OPERATIONS:")
    recent_ops = spark.sql(f"DESCRIBE HISTORY {table_name}").limit(5)
    recent_ops.select("version", "operation", "timestamp").show()

create_optimization_dashboard("delta_demo.sales_data")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 8: Best Practices and Guidelines

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimization Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC #### **When to Use OPTIMIZE**
# MAGIC
# MAGIC  **Optimize when:**
# MAGIC - File count > 50-100 per partition
# MAGIC - Average file size < 128MB
# MAGIC - Query performance degrades
# MAGIC - After many small writes
# MAGIC
# MAGIC  **Don't optimize when:**
# MAGIC - Files are already well-sized (128MB - 1GB)
# MAGIC - Table is write-heavy with frequent updates
# MAGIC - During peak query hours
# MAGIC
# MAGIC #### **When to Use Z-ORDER**
# MAGIC
# MAGIC  **Z-ORDER when:**
# MAGIC - Multiple columns in WHERE clauses
# MAGIC - Complex analytical queries
# MAGIC - Range queries on multiple dimensions
# MAGIC - High cardinality columns
# MAGIC
# MAGIC  **Avoid Z-ORDER with:**
# MAGIC - Single column queries (use partitioning instead)
# MAGIC - Very low cardinality columns
# MAGIC - Frequently changing query patterns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Column Selection for Z-ORDER

# COMMAND ----------

def analyze_column_cardinality(table_name):
    """Analyze column cardinality to guide Z-ORDER decisions"""
    
    print(f"Column Cardinality Analysis for {table_name}:")
    print("=" * 50)
    
    # Get numeric columns cardinality
    numeric_cols = ["customer_id", "product_id", "quantity", "unit_price", "total_amount"]
    categorical_cols = ["store_location", "payment_method"]
    
    for col_name in categorical_cols:
        try:
            cardinality = spark.sql(f"""
                SELECT COUNT(DISTINCT {col_name}) as distinct_count
                FROM {table_name}
            """).collect()[0]['distinct_count']
            
            total_rows = spark.sql(f"SELECT COUNT(*) as total FROM {table_name}").collect()[0]['total']
            
            selectivity = cardinality / total_rows if total_rows > 0 else 0
            
            print(f"{col_name:<20}: {cardinality:>6} distinct values ({selectivity:.1%} selectivity)")
            
            # Z-ORDER recommendation
            if 2 <= cardinality <= total_rows * 0.8:
                recommendation = "Good for Z-ORDER"
            elif cardinality < 2:
                recommendation = "Too low cardinality"
            else:
                recommendation = "Too high cardinality"
                
            print(f"{'':20}  → {recommendation}")
            
        except Exception as e:
            print(f"{col_name}: Error analyzing - {e}")

analyze_column_cardinality("delta_demo.sales_data")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimization Scheduling Strategy

# COMMAND ----------

def create_optimization_schedule():
    """Example optimization scheduling strategy"""
    
    optimization_schedule = [
        {
            "table_pattern": "*.fact_*",
            "frequency": "Daily 2 AM",
            "strategy": "OPTIMIZE + Z-ORDER",
            "condition": "file_count > 100 OR avg_file_size < 64MB"
        },
        {
            "table_pattern": "*.dim_*",
            "frequency": "Weekly Sunday 1 AM", 
            "strategy": "OPTIMIZE only",
            "condition": "file_count > 20"
        },
        {
            "table_pattern": "*.staging_*",
            "frequency": "After ETL completion",
            "strategy": "Auto-optimize enabled",
            "condition": "Always"
        }
    ]
    
    print("Optimization Scheduling Strategy:")
    print("=" * 60)
    
    for schedule in optimization_schedule:
        print(f"Tables: {schedule['table_pattern']}")
        print(f"  Frequency: {schedule['frequency']}")
        print(f"  Strategy: {schedule['strategy']}")
        print(f"  Condition: {schedule['condition']}")
        print()

create_optimization_schedule()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary

# COMMAND ----------

# MAGIC %md
# MAGIC #### Key Takeaways
# MAGIC
# MAGIC 1. **File Size Optimization**
# MAGIC    - Small files hurt query performance significantly
# MAGIC    - OPTIMIZE compacts small files into larger ones
# MAGIC    - Target 128MB - 1GB file sizes for optimal performance
# MAGIC    - Monitor file count and average size regularly
# MAGIC
# MAGIC 2. **Z-ORDER Benefits**
# MAGIC    - Co-locates related data for better data skipping
# MAGIC    - Most effective for multi-column filter queries
# MAGIC    - Choose columns with medium cardinality (not too high/low)
# MAGIC    - Combine with OPTIMIZE for maximum benefit
# MAGIC
# MAGIC 3. **Automation Strategies**
# MAGIC    - Enable auto-optimize for write-heavy workloads
# MAGIC    - Implement conditional optimization based on metrics
# MAGIC    - Schedule regular optimization during low-usage periods
# MAGIC    - Monitor optimization history and effectiveness
# MAGIC
# MAGIC 4. **Performance Monitoring**
# MAGIC    - Track file counts and sizes over time
# MAGIC    - Measure query performance before/after optimization
# MAGIC    - Use DESCRIBE DETAIL and DESCRIBE HISTORY for insights
# MAGIC    - Create optimization dashboards for key tables
# MAGIC
# MAGIC Next: **[Parquet to Delta Conversion →]($./11 - Parquet_to_Delta_Conversion)**