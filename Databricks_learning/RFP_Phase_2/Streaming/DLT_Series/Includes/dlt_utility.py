from pyspark.sql.functions import col, to_date, to_timestamp, lit, to_json, struct, current_timestamp
from Includes.config import *
from pyspark.sql.functions import *
import re
import requests
import json
from databricks.sdk.runtime import dbutils


def cleanup_data(pipeline_id):


    workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get() 
    api_url = f"{workspace_url}/api/2.0/pipelines/{pipeline_id}"

    headers = {
    "Authorization": f"Bearer {databricks_token}",
    "Content-Type": "application/json"
    }
    response = requests.delete(api_url, headers=headers)
    if response.status_code == 200:
        print("Pipeline deleted successfully.")
    else:
        print(f"Failed to delete pipeline: {response.status_code}")
        print(response.text)

    # delete catalog, sample data
    spark.sql("DROP CATALOG if exists `{CATALOG}` cascade")
    # in future if required to keep catalog and delete schemas
    # spark.sql("DROP SCHEMA if exists `{CATALOG}`.`{RAW_DATABASE}` cascade")
    # spark.sql("DROP SCHEMA if exists `{CATALOG}`.`{BRONZE_DATABASE}` cascade")
    # spark.sql("DROP SCHEMA if exists `{CATALOG}`.`{SILVER_DATABASE}` cascade")
    # spark.sql("DROP SCHEMA if exists `{CATALOG}`.`{GOLD_DATABASE}` cascade")


def print_pipeline_config():
    """
    Prints the key configuration for the Lakeflow / DLT pipeline.
    Helps the user understand where data is stored and how the layers are organized.
    """
    print("Current Pipeline Configuration:\n")
    print(f"Catalog (Unity Catalog)                               : {CATALOG}")
    print(f"Database (Schema) for Incremental Data                : {RAW_DATABASE}")
    print(f"Volume for Incremental Data (Storage)                 : {BASE_PATH}")
    print(f"Bronze Layer Database (Schema)                        : {BRONZE_DATABASE}")
    print(f"Silver Layer Database (Schema)                        : {SILVER_DATABASE}")
    print(f"Gold Layer Database (Schema)                          : {GOLD_DATABASE}")
    print(f"Notebook used in pipeline(store in current directory) : {PIPELINE_NOTEBOOK}")



def create_pipeline(pipeline_name, workspace_type):
    # ------------------------------
    # Set your workspace details
    workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get() 
    # ------------------------------
    # Detect AWS / Azure
    browser_host_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    if "azuredatabricks.net" in browser_host_name:
        cloud = "Azure"
        node_type_id = "Standard_F4"
    elif "databricks.com" in browser_host_name:
        cloud = "AWS"
        node_type_id = "m5d.large"
    else:
        raise Exception("Unknown cloud provider; cannot continue.")

    # Get current notebook path and dynamically resolve DLT notebook path
    current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    parent_dir = re.sub(r"(/DLT_series).*", r"\1", current_path)
    dlt_notebook_path = f"{parent_dir}/{PIPELINE_NOTEBOOK}"  

    # REST API to create pipeline
    api_url = f"{workspace_url}/api/2.0/pipelines"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }

    # Choose payload based on workspace_type
    if workspace_type.lower() == "trial edition":
        payload = {
            "name": pipeline_name,
            "pipeline_type": "WORKSPACE",
            "development": True,
            "continuous": False,
            "channel": "CURRENT",
            "libraries": [
                {
                    "notebook": {
                        "path": dlt_notebook_path
                    }
                }
            ],
            "edition": "ADVANCED",
            "serverless": True,
            "catalog": CATALOG,
            "configuration": {
                "catalog_name": CATALOG
            },
            "schema": "default"
        }
    else:
        payload = {
            "name": pipeline_name,
            "pipeline_type": "WORKSPACE",
            "clusters": [
                {
                    "label": "default",
                    "node_type_id": node_type_id,
                    "autoscale": {
                        "min_workers": 1,
                        "max_workers": 1,
                        "mode": "ENHANCED"
                    }
                }
            ],
            "development": True,
            "continuous": False,
            "channel": "CURRENT",
            "photon": False,
            "libraries": [
                {
                    "notebook": {
                        "path": dlt_notebook_path
                    }
                }
            ],
            "edition": "ADVANCED",
            "catalog": CATALOG,
            "configuration": {
                "catalog_name": CATALOG
            },
            "schema": "default"
        }

    # Send request
    response = requests.post(api_url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        print("Pipeline Created Successfully!")
        print(response.json())
        return response.json().get("pipeline_id")
    else:
        print(f"Failed to create pipeline. Status code: {response.status_code}")
        print(response.text)

