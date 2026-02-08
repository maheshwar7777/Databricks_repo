# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

import re
from databricks.sdk.runtime import *
from faker import Faker
import random
from datetime import datetime, timedelta
import pandas as pd

# Get current Databricks username to isolate catalog
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)

# Catalog & Schema Definitions
CATALOG = f"Delta_lake_RFP3_{clean_username}"
DEMO_SCHEMA = "delta_demo"
VOLUME_NAME = "sample_data"
BASE_PATH = f"{DEMO_SCHEMA}/{VOLUME_NAME}/raw_data/"

# Setup entities
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{DEMO_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.{DEMO_SCHEMA}.{VOLUME_NAME}")

# Generate data here...
fake = Faker()

# Rest of your data generation code
print("Data generation completed!")