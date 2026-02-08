import re
from databricks.sdk.runtime import *

# Get current Databricks username to isolate catalog
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)

# Catalog & Schema Definitions
CATALOG = f"Delta_lake_RFP3_{clean_username}"
DEMO_SCHEMA = "delta_demo"
VOLUME_NAME = "sample_data"
BASE_PATH = f"{DEMO_SCHEMA}/{VOLUME_NAME}/raw_data/"

# Data generation parameters
NUMBER_OF_RECORDS = 1000
BATCH_COUNT = 1