# Databricks notebook source
from faker import Faker
import csv
import datetime

fake = Faker()

def generate_customer_data(num_records):
    data = []
    for _ in range(num_records):
        data.append({
            'customer_id': fake.uuid4(),
            'name': fake.name(),
            'email': fake.email(),
            'phone_number': fake.phone_number(),
            'address': fake.address(),
            'created_at': fake.date_time_this_decade()
        })
    return data

def generate_product_data(num_records):
    data = []
    for _ in range(num_records):
        data.append({
            'product_id': fake.uuid4(),
            'product_name': fake.word(),
            'category': fake.word(),
            'price': fake.random_number(digits=5),
            'stock_quantity': fake.random_number(digits=3),
            'created_at': fake.date_time_this_decade()
        })
    return data

def generate_transaction_data(num_records):
    data = []
    for _ in range(num_records):
        data.append({
            'transaction_id': fake.uuid4(),
            'customer_id': fake.uuid4(),
            'product_id': fake.uuid4(),
            'quantity': fake.random_number(digits=2),
            'total_price': fake.random_number(digits=5),
            'transaction_date': fake.date_time_this_decade()
        })
    return data

def write_to_csv(data, filename):
    if data:
        keys = data[0].keys()
        with open(filename, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)

def generate_incremental_files():
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    customer_data = generate_customer_data(100)
    product_data = generate_product_data(100)
    transaction_data = generate_transaction_data(100)

    customer_file = f"/dbfs/tmp/customers_{timestamp}.csv"
    product_file = f"/dbfs/tmp/products_{timestamp}.csv"
    transaction_file = f"/dbfs/tmp/transactions_{timestamp}.csv"

    write_to_csv(customer_data, customer_file)
    write_to_csv(product_data, product_file)
    write_to_csv(transaction_data, transaction_file)

    dbutils.fs.cp(f"file:{customer_file}", f"dbfs:/tmp/customers_{timestamp}.csv")
    dbutils.fs.cp(f"file:{product_file}", f"dbfs:/tmp/products_{timestamp}.csv")
    dbutils.fs.cp(f"file:{transaction_file}", f"dbfs:/tmp/transactions_{timestamp}.csv")

generate_incremental_files()

# COMMAND ----------

import re
from databricks.sdk.runtime import *

# Get current Databricks username to isolate catalog
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
print(username)
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
print(clean_username)

# COMMAND ----------



# COMMAND ----------

# writing onboarding json file dynamically



import json

def create_entity(data_flow_id, data_flow_group, source_system, source_format, source_database, source_table, source_path_prod, source_schema_path, bronze_database_prod, bronze_table, bronze_table_path_prod, bronze_reader_options, bronze_cluster_by, bronze_data_quality_expectations_json_prod, bronze_database_quarantine_prod, bronze_quarantine_table, bronze_quarantine_table_path_prod, silver_database_prod, silver_table, silver_table_path_prod, silver_cdc_apply_changes, silver_cluster_by, silver_transformation_json_prod, silver_data_quality_expectations_json_prod):
    return {
        "data_flow_id": data_flow_id,
        "data_flow_group": data_flow_group,
        "source_system": source_system,
        "source_format": source_format,
        "source_details": {
            "source_database": source_database,
            "source_table": source_table,
            "source_path_prod": source_path_prod,
            "source_schema_path": source_schema_path
        },
        "bronze_database_prod": bronze_database_prod,
        "bronze_table": bronze_table,
        "bronze_table_path_prod": bronze_table_path_prod,
        "bronze_reader_options": bronze_reader_options,
        "bronze_cluster_by": bronze_cluster_by,
        "bronze_data_quality_expectations_json_prod": bronze_data_quality_expectations_json_prod,
        "bronze_database_quarantine_prod": bronze_database_quarantine_prod,
        "bronze_quarantine_table": bronze_quarantine_table,
        "bronze_quarantine_table_path_prod": bronze_quarantine_table_path_prod,
        "silver_database_prod": silver_database_prod,
        "silver_table": silver_table,
        "silver_table_path_prod": silver_table_path_prod,
        "silver_cdc_apply_changes": silver_cdc_apply_changes,
        "silver_cluster_by": silver_cluster_by,
        "silver_transformation_json_prod": silver_transformation_json_prod,
        "silver_data_quality_expectations_json_prod": silver_data_quality_expectations_json_prod
    }

entities = [
    create_entity(
        data_flow_id="100",
        data_flow_group="A1",
        source_system="mysql",
        source_format="cloudFiles",
        source_database="dlt_meta",
        source_table="customers",
        source_path_prod="/Volumes/notebook_scan/dlt_meta/dlt_meta_volume/Input_Files/Customers/",
        source_schema_path="/Volumes/notebook_scan/dlt_meta/dlt_meta_volume/Input_file_ddl/Customers/customers.ddl",
        bronze_database_prod="notebook_scan.dlt_meta",
        bronze_table="bronze_customers",
        bronze_table_path_prod="/Volumes/notebook_scan/dlt_meta/dlt_meta_volume/data/bronze/customers",
        bronze_reader_options={
            "cloudFiles.format": "csv",
            "cloudFiles.rescuedDataColumn": "_rescued_data",
            "header": "true"
        },
        bronze_cluster_by=["customer_id"],
        bronze_data_quality_expectations_json_prod="/Volumes/notebook_scan/dlt_meta/dlt_meta_volume/dqe/customers.json",
        bronze_database_quarantine_prod="notebook_scan.dlt_meta",
        bronze_quarantine_table="customers_quarantine",
        bronze_quarantine_table_path_prod="/Volumes/notebook_scan/dlt_meta/dlt_meta_volume/data/bronze/customers_quarantine",
        silver_database_prod="notebook_scan.dlt_meta",
        silver_table="silver_customers",
        silver_table_path_prod="/Volumes/notebook_scan/dlt_meta/dlt_meta_volume/data/silver/customers",
        silver_cdc_apply_changes={
            "keys": ["customer_id"],
            "sequence_by": "dmsTimestamp",
            "scd_type": "2",
            "apply_as_deletes": "Op = 'D'",
            "except_column_list": ["Op", "dmsTimestamp", "_rescued_data"]
        },
        silver_cluster_by=["customer_id"],
        silver_transformation_json_prod="/Volumes/notebook_scan/dlt_meta/dlt_meta_volume/transformations/silver_transformations.json",
        silver_data_quality_expectations_json_prod="/Volumes/notebook_scan/dlt_meta/dlt_meta_volume/dqe/customers_silver_dqe.json"
    )
    # Add more entities as needed
]

dbutils.fs.put("/dbfs/tmp/entities.json", json.dumps(entities, indent=4))


# another way to write the json file
# with open('entities.json', 'w') as f:
#     json.dump(entities, f, indent=4)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt_catalog.dlt_meta.bronze_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC drop table dlt_catalog.dlt_meta.bronze_customer;
# MAGIC drop table dlt_catalog.dlt_meta.bronze_dlt_meta_event_log;
# MAGIC drop table dlt_catalog.dlt_meta.bronze_product;
# MAGIC drop table dlt_catalog.dlt_meta.bronze_specs;
# MAGIC drop table dlt_catalog.dlt_meta.bronze_transaction;
# MAGIC drop table dlt_catalog.dlt_meta.customer_quarantine;
# MAGIC drop table dlt_catalog.dlt_meta.product_quarantine;
# MAGIC drop table dlt_catalog.dlt_meta.silver_dlt_meta_event_log;
# MAGIC drop table dlt_catalog.dlt_meta.silver_specs;
# MAGIC drop table dlt_catalog.dlt_meta.transaction_quarantine;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from dlt_catalog.dlt_meta.bronze_specs;
# MAGIC
# MAGIC select * from dlt_catalog.dlt_meta.silver_specs;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt_catalog.dlt_meta.silver_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt_catalog.dlt_meta.bronze_dlt_meta_event_log

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt_catalog.dlt_meta.silver_dlt_meta_event_log

# COMMAND ----------


import requests

workspace_url = "https://dbc-5c8bc4a5-351e.cloud.databricks.com/?o=4274458633864603>"
token = "dapif6ead16d0c82443a24c83727acc74496"

headers = {
    "Authorization": f"Bearer {token}"
}

response = requests.get(f"{workspace_url}/api/2.0/preview/scim/v2/Me", headers=headers)

print("Status Code:", response.status_code)
print("Response:", response.text)





# PAT token
# dapi920c5f5387f6305c99d28ecebcfdb6d2

# COMMAND ----------

# dlt_meta cli general comments :


# step 1 : Install python in local machine

# step 2 : databricks authentication commad:
databricks auth login --host https://dbc-5c8bc4a5-351e.cloud.databricks.com

Profile name :  dbc-5c8bc4a5-351e 


# Step 3 : dlt-meta librairy installation :
databricks labs install dlt-meta

    Note : Below running above command make below changes in config file for smooth dlt-meta library installation
    # --------------------------------------
    # Profile dbc-5c8bc4a5-351e was successfully saved


    # Change the value to profile value in below path config file :
    # C:\Users\maheshwar.kasireddy\.databricks\labs\dlt-meta\config


    # Before change in config file :
    # {
    #   "workspace_profile": "https://dbc-5c8bc4a5-351e.cloud.databricks.com"
    # }


    # Before change in config file :
    # after
    #   "workspace_profile": "dbc-5c8bc4a5-351e"
    # }

    # -------------------------------------------

# Step 4 : faces issues with onboarding (lissue is it trying to create new volumns and not taking exist)
databricks labs dlt-meta onboard 

# step 5 : run below command twice (One for Brionze and another for silver)
databricks labs dlt-meta deploy


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt_catalog.dlt_meta.bronze_product

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct product_name from dlt_catalog.dlt_meta.bronze_product

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dlt_catalog.dlt_meta.product_manufacture_cunty
# MAGIC as
# MAGIC (
# MAGIC select 'about' as product_name , 'USA' as country union all
# MAGIC select 'piece' as product_name , 'USA' as country union all
# MAGIC select 'state' as product_name , 'USA' as country union all
# MAGIC select 'administration' as product_name , 'USA' as country union all
# MAGIC select 'term' as product_name , 'USA' as country union all
# MAGIC select 'accept' as product_name , 'USA' as country union all
# MAGIC select 'develop' as product_name , 'USA' as country union all
# MAGIC select 'other' as product_name , 'USA' as country union all
# MAGIC select 'marriage' as product_name , 'USA' as country union all
# MAGIC select 'by' as product_name , 'USA' as country union all
# MAGIC select 'effect' as product_name , 'USA' as country union all
# MAGIC select 'must' as product_name , 'USA' as country union all
# MAGIC select 'ten' as product_name , 'USA' as country 
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt_catalog.dlt_meta.product_manufacture_cunty

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dlt_catalog.dlt_meta.bronze_customer;
# MAGIC drop table if exists dlt_catalog.dlt_meta.bronze_product;
# MAGIC drop table if exists dlt_catalog.dlt_meta.bronze_transaction;
# MAGIC drop table if exists dlt_catalog.dlt_meta.silver_customer;
# MAGIC drop table if exists dlt_catalog.dlt_meta.silver_product;
# MAGIC drop table if exists dlt_catalog.dlt_meta.silver_transaction;
# MAGIC drop table if exists dlt_catalog.dlt_meta.customer_quarantine;
# MAGIC drop table if exists dlt_catalog.dlt_meta.product_quarantine;
# MAGIC drop table if exists dlt_catalog.dlt_meta.transaction_quarantine;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt_catalog.dlt_meta.silver_product

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended dlt_catalog.dlt_meta.bronze_specs

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail dlt_catalog.dlt_meta.bronze_specs

# COMMAND ----------

# creating pipeline using rest api 

import requests
import json

def create_pipeline(workspace_url, token, pipeline_json):
    """
    Create a Databricks Lakeflow Declarative Pipeline using the REST API.

    Parameters:
        workspace_url (str): The base URL of your Databricks workspace (e.g., "https://<your-instance>.cloud.databricks.com").
        token (str): Your Databricks personal access token.
        pipeline_json (dict): The pipeline definition as a Python dictionary.

    Returns:
        dict: The API response as a dictionary.
    """
    api_url = f"{workspace_url}/api/2.0/pipelines"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = requests.post(api_url, headers=headers, data=json.dumps(pipeline_json))
    try:
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        return {
            "error": str(err),
            "status_code": response.status_code,
            "response": response.text
        }

def get_pipeline_details(workspace_url, token, pipeline_id):
    """
    Retrieve full details of a Databricks pipeline by pipeline_id.

    Parameters:
        workspace_url (str): The base URL of your Databricks workspace.
        token (str): Your Databricks personal access token.
        pipeline_id (str): The ID of the pipeline to retrieve.

    Returns:
        dict: The pipeline details as a dictionary.
    """
    api_url = f"{workspace_url}/api/2.0/pipelines/{pipeline_id}"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    response = requests.get(api_url, headers=headers)
    try:
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        return {
            "error": str(err),
            "status_code": response.status_code,
            "response": response.text
        }

# Example usage:
workspace_url = "https://dbc-5c8bc4a5-351e.cloud.databricks.com"
token = "dapiXXXXXXXXXXXXXXXXXXXXXXXX"  # Replace with your PAT

pipeline_json = {
    "name": "example_pipeline",
    "storage": "dbfs:/pipelines/example_pipeline",
    "configuration": {
        "key1": "value1"
    },
    "clusters": [
        {
            "label": "default",
            "num_workers": 2
        }
    ],
    "libraries": [
        {
            "notebook": {
                "path": "/Users/your.name@databricks.com/your_notebook"
            }
        }
    ]
    # Add other pipeline settings as needed
}

# Create pipeline and get response
result = create_pipeline(workspace_url, token, pipeline_json)
display(result)

# If pipeline creation is successful, fetch full pipeline details
if "pipeline_id" in result:
    pipeline_id = result["pipeline_id"]
    details = get_pipeline_details(workspace_url, token, pipeline_id)
    display(details)