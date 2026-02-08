# Databricks notebook source
# MAGIC %md
# MAGIC # Module 1: Delta Tables Core Concepts
# MAGIC
# MAGIC This module covers the fundamentals of creating and managing Delta tables using both SQL and Python approaches.
# MAGIC
# MAGIC #### Learning Objectives
# MAGIC - Create Delta tables using SQL DDL and Python DataFrame API
# MAGIC - Understand the difference between Delta and traditional formats
# MAGIC - Perform basic CRUD operations with ACID guarantees
# MAGIC - Implement schema enforcement and evolution

# COMMAND ----------

from Includes.config import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Check if the catalog exists, and create it if necessary
catalog_name = CATALOG  # Ensure CATALOG is set to the intended catalog name

# Create the catalog if it does not exist
spark.sql(
    f"CREATE CATALOG IF NOT EXISTS {catalog_name}"
)

# Set the catalog context
spark.sql(
    f"USE CATALOG {catalog_name}"
)

# Create the schema if it does not exist
spark.sql("CREATE SCHEMA IF NOT EXISTS delta_demo")

# COMMAND ----------

# MAGIC %md
# MAGIC #### What Makes Delta Different?
# MAGIC
# MAGIC | Feature | Traditional Parquet | Delta Lake |
# MAGIC |---------|-------------------|-----------|
# MAGIC | ACID Transactions | ❌ No | ✅ Yes |
# MAGIC | Schema Enforcement | ❌ No | ✅ Yes |
# MAGIC | Time Travel | ❌ No | ✅ Yes |
# MAGIC | Concurrent Writes | ❌ Conflicts | ✅ Safe |
# MAGIC | Data Quality | ❌ Manual | ✅ Built-in |
# MAGIC | Performance | ⚠️ Manual optimization | ✅ Auto-optimization |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Creating Delta Tables with SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1: CREATE TABLE with Schema Definition

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a customers table with explicit schema
# MAGIC CREATE OR REPLACE TABLE delta_demo.customers (
# MAGIC   customer_id BIGINT NOT NULL,
# MAGIC   first_name STRING NOT NULL,
# MAGIC   last_name STRING NOT NULL,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   registration_date DATE,
# MAGIC   customer_segment STRING DEFAULT 'Standard',
# MAGIC   total_spent DECIMAL(10,2) DEFAULT 0.00,
# MAGIC   is_active BOOLEAN DEFAULT true,
# MAGIC   last_login TIMESTAMP,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (customer_segment)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'delta.feature.allowColumnDefaults' = 'supported'
# MAGIC )
# MAGIC COMMENT 'Customer master data with ACID guarantees'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe the table structure
# MAGIC DESCRIBE EXTENDED delta_demo.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: CREATE TABLE AS SELECT (CTAS)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create orders table from a query result
# MAGIC CREATE OR REPLACE TABLE delta_demo.orders
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (order_status)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC )
# MAGIC AS
# MAGIC SELECT 
# MAGIC   1001 as order_id,
# MAGIC   1 as customer_id,
# MAGIC   CAST('2024-01-15 10:30:00' AS TIMESTAMP) as order_date,
# MAGIC   CAST(299.99 AS DECIMAL(10,2)) as order_amount,
# MAGIC   'Completed' as order_status,
# MAGIC   '123 Main St, Anytown' as shipping_address,
# MAGIC   CURRENT_TIMESTAMP() as created_at
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   1002, 2, CAST('2024-01-16 14:20:00' AS TIMESTAMP), 
# MAGIC   CAST(149.50 AS DECIMAL(10,2)), 'Pending', 
# MAGIC   '456 Oak Ave, Somewhere', CURRENT_TIMESTAMP()

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from delta_demo.orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Creating Delta Tables with Python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1: DataFrame API with Schema

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DecimalType,
    IntegerType, BooleanType, TimestampType
)
import decimal

# Define explicit schema for products
products_schema = StructType([
    StructField("product_id", LongType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DecimalType(10,2), True),
    StructField("cost", DecimalType(10,2), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("reorder_level", IntegerType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("last_updated", TimestampType(), True)
])


from datetime import datetime
import decimal

# Create sample products data with datetime objects instead of strings
sample_products = [
    (2001, "Wireless Bluetooth Headphones", "Electronics", "TechBrand", decimal.Decimal("79.99"), decimal.Decimal("45.00"), 150, 20, True, datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 15, 10, 0, 0)),
    (2002, "Cotton T-Shirt", "Clothing", "FashionCo", decimal.Decimal("24.99"), decimal.Decimal("12.50"), 300, 50, True, datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 15, 10, 0, 0)),
    (2003, "Python Programming Book", "Books", "TechPublish", decimal.Decimal("49.99"), decimal.Decimal("25.00"), 75, 10, True, datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 15, 10, 0, 0)),
    (2004, "Stainless Steel Water Bottle", "Home", "EcoLife", decimal.Decimal("34.99"), decimal.Decimal("18.00"), 200, 25, True, datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 15, 10, 0, 0))
]

products_df = spark.createDataFrame(sample_products, products_schema)

# Write as Delta table
(products_df.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .partitionBy("category")
 .saveAsTable("delta_demo.products"))

# Set table properties
spark.sql("""
ALTER TABLE delta_demo.products SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

print("Products table created successfully with Python!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: DeltaTable.create() for Advanced Control

# COMMAND ----------

# Create inventory tracking table with generated columns
DeltaTable.createIfNotExists(spark) \
  .tableName("delta_demo.inventory") \
  .addColumn("inventory_id", "BIGINT", nullable=False) \
  .addColumn("product_id", "BIGINT", nullable=False) \
  .addColumn("warehouse_location", "STRING") \
  .addColumn("quantity_on_hand", "INT") \
  .addColumn("quantity_reserved", "INT") \
  .addColumn("available_quantity", "INT", generatedAlwaysAs="quantity_on_hand - quantity_reserved") \
  .addColumn("last_count_date", "DATE") \
  .addColumn("created_at", "TIMESTAMP") \
  .partitionedBy("warehouse_location") \
  .property("delta.enableChangeDataFeed", "true") \
  .property("delta.constraints.valid_quantity", "quantity_on_hand >= 0") \
  .execute()

print("Inventory table created with computed columns and constraints!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 3: Understanding Schema Enforcement

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schema Enforcement in Action

# COMMAND ----------

# This will FAIL due to schema enforcement
try:
    bad_data = [("invalid_id", "John", "Doe")]  # customer_id should be BIGINT, not STRING
    bad_df = spark.createDataFrame(bad_data, ["customer_id", "first_name", "last_name"])
    bad_df.write.format("delta").mode("append").saveAsTable("delta_demo.customers")
except Exception as e:
    print(f"Schema enforcement prevented bad data: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Correct Data Insertion

# COMMAND ----------

# Insert valid customer data using SQL
spark.sql("""
INSERT INTO delta_demo.customers 
(customer_id, first_name, last_name, email, phone, registration_date, customer_segment, total_spent, is_active)
VALUES 
  (1, 'John', 'Doe', 'john.doe@email.com', '+1-555-0123', '2024-01-15', 'VIP', 2500.00, true),
  (2, 'Jane', 'Smith', 'jane.smith@email.com', '+1-555-0456', '2024-01-16', 'Premium', 1800.00, true),
  (3, 'Bob', 'Johnson', 'bob.j@email.com', '+1-555-0789', '2024-01-17', 'Standard', 450.00, true)
""")

# Insert using Python DataFrame
from datetime import date

from decimal import Decimal

additional_customers = [
    (4, 'Alice', 'Williams', 'alice.w@email.com', '+1-555-1111', date(2024, 1, 18), 'Premium', Decimal('1200.00'), True, None, None),
    (5, 'Charlie', 'Brown', 'charlie.b@email.com', '+1-555-2222', date(2024, 1, 19), 'VIP', Decimal('3200.00'), True, None, None)
]

schema_subset = StructType([
    StructField("customer_id", LongType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("registration_date", DateType(), True),  # Changed to DateType
    StructField("customer_segment", StringType(), True),
    StructField("total_spent", DecimalType(10,2), True),
    StructField("is_active", BooleanType(), True),
    StructField("last_login", TimestampType(), True),
    StructField("created_at", TimestampType(), True)
])

customers_df = spark.createDataFrame(additional_customers, schema_subset)



customers_df = spark.createDataFrame(additional_customers, schema_subset)
customers_df.write.format("delta").mode("append").saveAsTable("delta_demo.customers")
print("Customer data inserted successfully!")


# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from delta_demo.customers

# COMMAND ----------

# MAGIC %md
# MAGIC #### Part 4: Basic CRUD Operations with ACID

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query customers by segment
# MAGIC SELECT customer_segment, COUNT(*) as customer_count, AVG(total_spent) as avg_spending
# MAGIC FROM delta_demo.customers
# MAGIC GROUP BY customer_segment
# MAGIC ORDER BY avg_spending DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPDATE Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update customer spending (this creates a new version)
# MAGIC UPDATE delta_demo.customers 
# MAGIC SET 
# MAGIC   total_spent = total_spent + 500.00,
# MAGIC   last_login = CURRENT_TIMESTAMP(),
# MAGIC   customer_segment = CASE 
# MAGIC     WHEN total_spent + 500 >= 3000 THEN 'VIP'
# MAGIC     WHEN total_spent + 500 >= 1500 THEN 'Premium'
# MAGIC     ELSE 'Standard'
# MAGIC   END
# MAGIC WHERE customer_id IN (2, 3)

# COMMAND ----------

# Python UPDATE using DeltaTable API
deltaTable = DeltaTable.forName(spark, "delta_demo.customers")

deltaTable.update(
    condition = col("customer_segment") == "Standard",
    set = {
        "last_login": current_timestamp(),
        "customer_segment": lit("Standard_Updated")
    }
)

print("Customers updated using Python DeltaTable API!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DELETE Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Soft delete: Mark customers as inactive
# MAGIC UPDATE delta_demo.customers 
# MAGIC SET is_active = false 
# MAGIC WHERE total_spent < 500

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Hard delete: Remove inactive customers
# MAGIC DELETE FROM delta_demo.customers 
# MAGIC WHERE is_active = false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Schema Evolution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding New Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add new columns to existing table
# MAGIC ALTER TABLE delta_demo.customers 
# MAGIC ADD COLUMNS (
# MAGIC   loyalty_tier STRING ,
# MAGIC   referral_code STRING,
# MAGIC   marketing_consent BOOLEAN 
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution with DataFrame Writes

# COMMAND ----------

from decimal import Decimal
from datetime import date, datetime

# Get the current schema from the table
table_schema = spark.table("delta_demo.customers").schema

# Map your new customer data by column name for clarity
customer_dict = {
    "customer_id": 6,
    "first_name": "Emma",
    "last_name": "Davis",
    "email": "emma.d@email.com",
    "phone": "+1-555-3333",
    "registration_date": date(2024, 1, 20),
    "customer_segment": "VIP",
    "total_spent": Decimal("4500.00"),
    "is_active": True,
    "last_login": None,
    "created_at": None,
    "loyalty_tier": "Gold",
    "referral_code": "REF123",
    "marketing_consent": True
    # Add more fields here if your table schema has more columns
}

# Build the data tuple in the exact order of the table schema, filling missing with None
data_tuple = tuple(
    customer_dict.get(field.name, None)
    for field in table_schema
)

new_customer_df = spark.createDataFrame([data_tuple], table_schema)

new_customer_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("delta_demo.customers")

display(new_customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary
# MAGIC
# MAGIC In this module, you learned:
# MAGIC
# MAGIC - **Multiple ways to create Delta tables** using SQL DDL and Python APIs
# MAGIC - **Schema enforcement** prevents data quality issues automatically
# MAGIC - **ACID transactions** ensure data consistency during concurrent operations
# MAGIC - **Schema evolution** allows safe table structure changes
# MAGIC
# MAGIC
# MAGIC Next: **[Managed vs External Tables →]($./02-Managed_vs_External_Tables)**