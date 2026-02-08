# Databricks notebook source
# MAGIC %pip install iso3166 Faker
# MAGIC %restart_python

# COMMAND ----------

from faker import Faker
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row
import pandas as pd
import json
import os
from Includes.config import *
from Includes.utils import *

# COMMAND ----------

spark.sql(f'drop catalog if exists {catalog_name} cascade')

# COMMAND ----------

spark.sql(f'''create catalog if not exists {catalog_name}''')
spark.sql(f'''create schema if not exists {catalog_name}.{bronze_schema_name}''')
spark.sql(f'''create volume if not exists {catalog_name}.{bronze_schema_name}.{volume_name}''')

# COMMAND ----------

spark.sql(f'''create schema if not exists {catalog_name}.{silver_schema_name}''')

# COMMAND ----------

spark.sql(f'''create schema if not exists {catalog_name}.{gold_schema_name}''')