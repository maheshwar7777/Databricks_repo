import re
from databricks.sdk.runtime import *

# Get current Databricks username to isolate catalog
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)


# Catalog & Schema Definitions

CATALOG           = "dlt_catalog"   # Unity Catalog catalog name (unique per user)  # using existing catalog
DLT_META_DATABASE      = "dlt_meta" # Used for raw data 



# Volume & Data Paths
VOLUME_NAME       = "dlt_meta_volume"                                          # Unity Catalog volume name
BASE_PATH         = f"/Volumes/{CATALOG}/{DLT_META_DATABASE}/{VOLUME_NAME}/input_files"     # Raw data input path
SCHEMA_PATH       = f"/Volumes/{CATALOG}/{DLT_META_DATABASE}/{VOLUME_NAME}/input_file_ddl"            # Schema inference path
DQE               = f"/Volumes/{CATALOG}/{DLT_META_DATABASE}/{VOLUME_NAME}/dqe"             # data quality expections file path
ON_BOARDING_JSON  = f"/Volumes/{CATALOG}/{DLT_META_DATABASE}/{VOLUME_NAME}/onboarding"      # Onboarding json file path
TRANSFORMATION    = f"/Volumes/{CATALOG}/{DLT_META_DATABASE}/{VOLUME_NAME}/transformation"  # Transformation path


# Pipeline Notebook & Table list
# PIPELINE_NOTEBOOK = "01-Python for Delta Live Tables"                               # Path to the DLT logic notebook
TABLE_LIST        = ["customer", "product", "transaction"]  # Datasets to process


# Data generation paramater
RESET_ALL_DATA     = "False"   # If True, deletes all previously generated data
NUMBER_OF_RECORDS  = 500      # Number of records per batch
BATCH_WAIT         = 30        # Seconds to wait between batches
BATCH_COUNT        = 1         # Number of batches to generate


# print config_values
def print_config_values():
    """
    Prints the key configuration for the DLT_META
    Helps the user understand where data is stored and how the layers are organized.
    """
    print("Current Pipeline Configuration:\n")
    print(f"Catalog (Unity Catalog)                               : {CATALOG}")
    print(f"Database (Schema) for Incremental Data                : {DLT_META_DATABASE}")
    print(f"Volume for Incremental Data (Storage)                 : {BASE_PATH}")
    print(f"Volume for input file ddl (Storage)                   : {SCHEMA_PATH}")
    print(f"Volume for DQE (Storage)                              : {DQE}")
    print(f"Volume for on boarding json (Storage)                 : {ON_BOARDING_JSON}")
    print(f"Volume for transformation (Storage)                   : {TRANSFORMATION}")
    # print(f"Notebook used in pipeline(store in current directory) : {PIPELINE_NOTEBOOK}")