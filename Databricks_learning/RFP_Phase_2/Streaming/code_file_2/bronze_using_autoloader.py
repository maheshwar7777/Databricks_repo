# Databricks notebook source
from pyspark.sql.types import StructType
import json
import dlt
from pyspark.sql.functions import input_file_name, current_timestamp
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, TimestampType, DateType, DecimalType
from pyspark.sql.functions import current_timestamp, col, json_tuple
from config import *

# COMMAND ----------

def load_schema_from_volume(path, table_name: str) -> StructType:
    schema_path = f"{path}{table_name}_schema.json"
    schema_json = dbutils.fs.head(schema_path)
    return StructType.fromJson(json.loads(schema_json))

# COMMAND ----------

def create_bronze_table(table_name):

    schema = load_schema_from_volume(SCHEMA_PATH, table_name)
    """Creates a Bronze DLT table for each dataset."""
    @dlt.table(
        name=f"{CATALOG}.{BRONZE_DATABASE}.{table_name}",  
        comment=f"Bronze table for {table_name} data",
    )
    def bronze_data():
        return (
            spark.readStream
            .format("cloudFiles")  
            .option("cloudFiles.format", "csv")  
            .option("cloudFiles.inferSchema", "false")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", f"{AUTOLOADER_BRONZE_CHECKPOINT_PATH}{table_name}/")    
            .schema(schema)  
            .load(f"{BASE_PATH}{table_name}/") 
            .withColumn("ingestiontime", current_timestamp())
        )

for table in TABLE_LIST:
    create_bronze_table(table)