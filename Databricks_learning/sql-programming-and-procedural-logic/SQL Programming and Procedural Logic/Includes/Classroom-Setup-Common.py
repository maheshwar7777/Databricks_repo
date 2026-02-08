# Databricks notebook source
# MAGIC %pip install --quiet databricks-sql-connector

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../../Includes/_common

# COMMAND ----------

## CREATE THE PYTHON DA OBJECT FROM THE DBACADEMY.OPS.META TABLE
DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

# Reset to use default catalog and schema created by the DA object in Python

# Set current catalog
spark.sql(f"USE CATALOG {DA.catalog_name}")

# Set current schema within the selected catalog
spark.sql(f"USE SCHEMA {DA.schema_name}")

# COMMAND ----------

# # 🔒 Establish Secondary-User SQL Warehouse Connection (DA.connection)

# warehouse = next(
#     filter(
#         lambda w: w.name == 'shared_warehouse',
#         DA.workspace.warehouses.list()
#     )
# )
# from databricks import sql
# DA.connection = sql.connect(
#     warehouse.odbc_params.hostname,
#     warehouse.odbc_params.path,
#     dbutils.secrets.get(DA.secrets, 'secondary_token')
# )

# # DA.user_catalog_name already set above!
# DA.secondary_principal_catalog_name = DA.uc_safename(DA.pseudonym)

# # Save DA.secondary_principal_catalog_name to Spark session config
# spark.conf.set("DA.secondary_principal_catalog_name", DA.secondary_principal_catalog_name)

# # Clean up old catalog for this secondary principal (if needed)
# spark.sql(f'DROP CATALOG IF EXISTS `{DA.secondary_principal_catalog_name}` CASCADE')

# COMMAND ----------

# # 🔎 Lookup Data Source ID for SQL Warehouse 'shared_warehouse'

# from databricks.sdk import WorkspaceClient

# # Use Databricks SDK to get the data source (UUID) ID for SQL queries
# w = WorkspaceClient()
# data_source_id = None

# for ds in w.data_sources.list():
#     if ds.name == 'shared_warehouse':
#         data_source_id = ds.id
#         break

# if not data_source_id:
#     raise ValueError("Data source ID for 'shared_warehouse' not found! Check if it exists.")

# # Assign values for Warehouse Name and Warehouse ID
# DA.warehouse_id = data_source_id
# spark.conf.set("DA.warehouse_id", DA.warehouse_id)  # Save DA.warehouse_id to Spark session config

# DA.warehouse_name = warehouse.name
# spark.conf.set("DA.warehouse_name", DA.warehouse_name)  # Save DA.warehouse_name to Spark session config

# COMMAND ----------

temo = 'orders'

# COMMAND ----------

import json
context_json = dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson()

# Parse the JSON string into a Python dictionary
context_dict = json.loads(context_json)
user_email = context_dict.get('tags', {}).get('user') or context_dict.get('attributes', {}).get('user')
meta_table_name = 'meta'+user_email.split('@')[0].replace(".","_")

# COMMAND ----------

# %sql
# -- THIS CREATES THE DA SQL VARIABLE USING FOR USER INFORMATION FROM THE DBADCADEMY.OPS.META TABLE. 
# -- USE THIS LIKE THE PYTHON DA OBJECT BUT WITHIN SQL SO THIS WILL RUN ON A SQL WAREHOUSE.

# -- Create a temp view storing information from the obs table.
# CREATE OR REPLACE TEMP VIEW user_info AS
# SELECT map_from_arrays(collect_list(replace(key,'.','_')), collect_list(value))
# FROM impetuslearning.ops.meta;

# -- Create SQL dictionary var (map)
# DECLARE OR REPLACE DA MAP<STRING,STRING>;

# -- Set the temp view in the DA variable
# SET VAR DA = (SELECT * FROM user_info);

# DROP VIEW IF EXISTS user_info;

# COMMAND ----------


# -- THIS CREATES THE DA SQL VARIABLE USING FOR USER INFORMATION FROM THE DBADCADEMY.OPS.META TABLE. 
# -- USE THIS LIKE THE PYTHON DA OBJECT BUT WITHIN SQL SO THIS WILL RUN ON A SQL WAREHOUSE.

# -- Create a temp view storing information from the obs table.
meta_table = f"impetuslearning.ops.{meta_table_name}"

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW user_info AS
SELECT map_from_arrays(
    collect_list(replace(key, '.', '_')),
    collect_list(value)
)
FROM {meta_table}
""")




# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create SQL dictionary var (map)
# MAGIC DECLARE OR REPLACE DA MAP<STRING,STRING>;
# MAGIC
# MAGIC -- Set the temp view in the DA variable
# MAGIC SET VAR DA = (SELECT * FROM user_info);
# MAGIC
# MAGIC DROP VIEW IF EXISTS user_info;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reset to use default catalog and schema created by the DA object in SQL
# MAGIC
# MAGIC -- Set current catalog
# MAGIC USE CATALOG ${DA.catalog_name};
# MAGIC
# MAGIC -- Set current schema within the selected catalog
# MAGIC USE SCHEMA ${DA.schema_name};

# COMMAND ----------

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# COMMAND ----------

# Write your custom common code here

# COMMAND ----------

# 👤 Get unique user identifier/email for personalization

try:
    DA.username = spark.sql("SELECT current_user()").first()[0]
except Exception:
    DA.username = "unknown_user"

# Save DA.username to Spark session config
spark.conf.set("DA.username", DA.username)

# COMMAND ----------

# 🏠 Set User Home Directory Path
DA.user_home_path = f"/Workspace/Users/{DA.username}/"

# Save DA.user_home_path to Spark session config
spark.conf.set("DA.user_home_path", DA.user_home_path)

# COMMAND ----------

# 🗃️ Derive user_id and personalized catalog name (company policy)

import re
def get_user_id(username):
    # Typical approach = everything before first . or @, alphanumeric only
    username = username.strip().lower()
    m = re.match(r"([^@.]+)", username)
    return m.group(1) if m else username.replace("@", "_").replace(".", "_")

DA.user_id = get_user_id(DA.username)
DA.user_catalog_name = f"{DA.user_id}_catalog"

# Save DA.user_catalog_name to Spark session config
spark.conf.set("DA.user_catalog_name", DA.user_catalog_name)