# Databricks notebook source
# MAGIC %md
# MAGIC ### Call data generation notebook

# COMMAND ----------

# MAGIC %run ./data_generation

# COMMAND ----------

# run below fucntion to generate sample csv files
# expected parameters are customer, product, transaction and number of rows

generate_incremental_files(input_entity_cust='customer', input_entity_prod='product', input_entity_trans='transaction', row_cnt=500)

# COMMAND ----------

import datetime
# from config import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json

# %load_ext autoreload
# %autoreload 2

# COMMAND ----------

# Create a dict to holder flow_id for respective entity

table_list_flow_id =[ {"flow_id": (100+1+TABLE_LIST.index(i)), "table_nm": i} for i in TABLE_LIST ]

print(f"Entity info : {table_list_flow_id}")

# for i in table_list_flow_id:
#     print(i['flow_id'])
#     print(i['table_nm'])

# COMMAND ----------

# assign entity column detials

# customer_col = ['customer_id','name','email','phone_number','address','created_at','Op','_rescued_data']
# product_col = ['product_id','product_name','category','price','stock_quantity','created_at','Op','_rescued_data']
# transaction_col = ['transaction_id','customer_id','product_id','quantity','total_price','transaction_date','created_at','Op','_rescued_data']

customer_col = ["customer_id","name","email","phone_number","address","created_at","Op","_rescued_data","date_format(created_at, 'dd-MM-yyyy') as trans_created_dt", "current_date() as load_date", "'silver' as source_layer"]
product_col = ["product_id","product_name","category","price","stock_quantity","created_at","Op","_rescued_data","date_format(created_at, 'dd-MM-yyyy') as trans_created_dt", "current_date() as load_date", "'silver' as source_layer"]
transaction_col = ["transaction_id","customer_id","product_id","quantity","total_price","transaction_date","created_at","Op","_rescued_data","date_format(created_at, 'dd-MM-yyyy') as trans_created_dt", "current_date() as load_date", "'silver' as source_layer"]

entity_col_names = {
    "customer": customer_col,
    "product": product_col,
    "transaction": transaction_col
}

# print(f"entity column level info : {entity_col_names}")

for i in entity_col_names:
    print(f"entity column level info : {i} : {entity_col_names[i]}")


# COMMAND ----------

# funtion to create entity schema ddl file
def create_entity_schema(entity_nm:str):

    if(entity_nm == "customer"):

        # Read the CSV file with inferred schema
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{BASE_PATH}/{entity_nm}/*.csv")

        # Get the schema and format it as required
        schema_info = ", ".join([f"{field.name} : {field.dataType.simpleString()}" for field in df.schema.fields])
        
        return schema_info
    
    elif(entity_nm == "product"):

        # Read the CSV file with inferred schema
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{BASE_PATH}/{entity_nm}/*.csv")

        # Get the schema and format it as required
        schema_info = ", ".join([f"{field.name} : {field.dataType.simpleString()}" for field in df.schema.fields])
        
        return schema_info

    elif(entity_nm == "transaction"):

        # Read the CSV file with inferred schema
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{BASE_PATH}/{entity_nm}/*.csv")

        # Get the schema and format it as required
        schema_info = ", ".join([f"{field.name} : {field.dataType.simpleString()}" for field in df.schema.fields])
        
        return schema_info        

    else:
        pass


#Generate input csv file schema dll file
for entity_nm in TABLE_LIST:
    data_schema_ddl=create_entity_schema(entity_nm)

    if data_schema_ddl is None:
        continue

    file_path = f"{SCHEMA_PATH}/{entity_nm}/"
    
    try:
        files = dbutils.fs.ls(file_path)
    except Exception:
        dbutils.fs.mkdirs(file_path)
        files = []

    matching_files = [f for f in files if f.name.endswith(".ddl")]
        
    if not matching_files:
        dbutils.fs.put(f"{SCHEMA_PATH}/{entity_nm}/{entity_nm}.ddl", data_schema_ddl, overwrite=True)
        print(f"{entity_nm} ddl file is created")
    else :
        print(f"{entity_nm} ddl file already exists")


# COMMAND ----------

#  funtion to create bronze dqe json file
def create_entity_bronze_dqe(entity_nm:str):
  
        if entity_nm == "customer":
            return {
                    "expect_or_drop": {
                        "no_rescued_data": "_rescued_data IS NULL",
                        "valid_customer_id": "customer_id IS NOT NULL"
                    },
                    "expect_or_quarantine": {
                        "quarantine_rule": "_rescued_data IS NOT NULL OR customer_id IS NULL"
                    }
                }
            
        elif entity_nm == "product":
           return {
                    "expect_or_drop": {
                        "no_rescued_data": "_rescued_data IS NULL",
                        "valid_product_id": "product_id IS NOT NULL"
                    },
                    "expect_or_quarantine": {
                        "quarantine_rule": "_rescued_data IS NOT NULL OR product_id IS NULL"
                    }
                }
        
        elif entity_nm == "transaction":
           return {
                    "expect_or_drop": {
                        "no_rescued_data": "_rescued_data IS NULL",
                        "valid_transaction_id": "transaction_id IS NOT NULL",
                        "valid_customer_id": "customer_id IS NOT NULL"
                    },
                    "expect_or_quarantine": {
                        "quarantine_rule": "_rescued_data IS NOT NULL OR transaction_id IS NULL OR customer_id IS NULL"
                    }
                }
        else : 
            pass


# Generate on bronze dqe json file in target path
for entity_nm in TABLE_LIST:
    data_bronze_dqe = create_entity_bronze_dqe(entity_nm)

    if data_bronze_dqe != None:
        dbutils.fs.put(f"{DQE}/{entity_nm}_bronze_dqe.json", json.dumps(data_bronze_dqe, indent=4), overwrite=True)
        print(f"{entity_nm} bronze DQE file is created")



# COMMAND ----------

#  funtion to create silver dqe json file
def create_entity_silver_dqe(entity_nm:str):
  
        if entity_nm == "customer":
            return {
                    "expect_or_drop": {
                        "valid_customer_id": "customer_id IS NOT NULL"
                    }
                }
            
        elif entity_nm == "product":
           return {
                    "expect_or_drop": {
                        "valid_product_id": "product_id IS NOT NULL"
                    }
                }
        elif entity_nm == "transaction":
           return {
                    "expect_or_drop": {
                        "valid_transaction_id": "transaction_id IS NOT NULL",
                        "valid_customer_id": "customer_id IS NOT NULL"
                    }
                }
        else : 
            pass


# Generate on silver dqe json file in target path
for entity_nm in TABLE_LIST:
    data_silver_dqe = create_entity_silver_dqe(entity_nm)

    if data_silver_dqe != None:
        dbutils.fs.put(f"{DQE}/{entity_nm}_silver_dqe.json", json.dumps(data_silver_dqe, indent=4), overwrite=True)
        print(f"{entity_nm} silver DQE file is created")



# COMMAND ----------

# funtion to create silver transformation json file

def create_silver_trans(flow_id:str, table_nm:str, slt_expr:list):
    return {
            "data_flow_id": flow_id,
            "source_table": f"bronze_{table_nm}",
            "target_table": f"silver_{table_nm}",
            "select_exp": slt_expr
        }


# Generate on silver transformation json file in target path
data_silver_trans=[]

for entity_nm in table_list_flow_id:
    data_silver_trans.append(create_silver_trans( str(entity_nm['flow_id']), entity_nm['table_nm'], entity_col_names[entity_nm['table_nm']]))


dbutils.fs.put(f"{TRANSFORMATION}/silver_transformation.json", json.dumps(data_silver_trans, indent=4), overwrite=True)
print(f"Transformation file is created")

# COMMAND ----------

# funtion to create on boarding json file for mulitple entities

def create_entity(data_flow_id, source_table):
    return {
        "data_flow_id": data_flow_id,
      "data_flow_group": "A1",
      "source_system": "mysql",
      "source_format": "cloudFiles",
      "source_details": {
         "source_database": f"{DLT_META_DATABASE}",
         "source_table": source_table,
         "source_path_prod": f"{BASE_PATH}/{source_table}/",
         "source_schema_path": f"{SCHEMA_PATH}/{source_table}/{source_table}.ddl"
      },
      "bronze_database_prod": f"{CATALOG}.{DLT_META_DATABASE}",
      "bronze_table": f"bronze_{source_table}",
      "bronze_table_path_prod": f"/Volumes/{CATALOG}/{DLT_META_DATABASE}/{VOLUME_NAME}/bronze/{source_table}",
      "bronze_reader_options": {
         "cloudFiles.format": "csv",
         "cloudFiles.rescuedDataColumn": "_rescued_data",
         "header": "true"
      },
      "bronze_cluster_by":[f"{source_table}_id"],
      "bronze_data_quality_expectations_json_prod": f"{DQE}/{source_table}_bronze_dqe.json",
      "bronze_database_quarantine_prod": f"{CATALOG}.{DLT_META_DATABASE}",
      "bronze_quarantine_table": f"{source_table}_quarantine",
      "bronze_quarantine_table_path_prod": f"/Volumes/{CATALOG}/{DLT_META_DATABASE}/{VOLUME_NAME}/bronze/{source_table}_quarantine",
      "silver_database_prod": f"{CATALOG}.{DLT_META_DATABASE}",
      "silver_table": f"silver_{source_table}",
      "silver_table_path_prod": f"/Volumes/{CATALOG}/{DLT_META_DATABASE}/{VOLUME_NAME}/silver/{source_table}",
      "silver_cdc_apply_changes": {
         "keys": [
            f"{source_table}_id"
         ],
         "sequence_by": "created_at",
         "scd_type": "2",
         "apply_as_deletes": "Op = 'D'",
         "except_column_list": [
            "created_at",
            "Op",
            "_rescued_data"
         ]
      },
      "silver_cluster_by":[f"{source_table}_id"],
      "silver_transformation_json_prod": f"{TRANSFORMATION}/silver_transformation.json",
      "silver_data_quality_expectations_json_prod": f"{DQE}/{source_table}_silver_dqe.json"
    }



# generate on boarding json file in target path
data=[]

for entity_nm in table_list_flow_id:
    data.append(create_entity(str(entity_nm['flow_id']), entity_nm['table_nm']))


dbutils.fs.put(f"{ON_BOARDING_JSON}/on_boarding_json_file.json", json.dumps(data, indent=4), overwrite=True)
print(f"On boarding json file is created")