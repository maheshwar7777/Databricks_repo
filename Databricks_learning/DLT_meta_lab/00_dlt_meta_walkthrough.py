# Databricks notebook source
# MAGIC %pip install iso3166 Faker
# MAGIC %pip install dlt-meta 
# MAGIC %restart_python
# MAGIC # %load_ext autoreload
# MAGIC # %autoreload 2

# COMMAND ----------

from include.config import *
from include.dlt_meta_utility import *
%load_ext autoreload
%autoreload 2

# COMMAND ----------

# MAGIC %run ./include/create_onboarding_json_file

# COMMAND ----------

print_config_values()

# COMMAND ----------

# MAGIC %run ./01-DLT_META_onboarding_step_up

# COMMAND ----------

# cleanup_data(spark,CATALOG,DLT_META_DATABASE )