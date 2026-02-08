# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# COMMAND ----------

# Write your custom code from here

# COMMAND ----------

# 🏗️ Create user catalog and bronze/silver/gold schemas, if not exist

catalog_name = DA.user_catalog_name
schemas = ["bronze", "silver", "gold"]

# Create catalog if not present
existing_catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
if catalog_name not in existing_catalogs:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

# Create schemas if not present
for schema in schemas:
    # Use 'namespace' attribute — it is standard for SHOW SCHEMAS IN Unity Catalog!
    existing_schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()]
    if schema not in existing_schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")

# COMMAND ----------

# 📑 Copy TPC-H tables from samples.tpch to user bronze schema

bronze_db = f"{catalog_name}.bronze"
tables = [r.tableName for r in spark.sql("SHOW TABLES IN samples.tpch").collect()]
existing_tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {bronze_db}").collect()]

for table_name in tables:
    if table_name not in existing_tables:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_db}.{table_name} AS SELECT * FROM samples.tpch.{table_name}")

# COMMAND ----------

# 🛡️ Grant privileges so only this user (my_user) can use their catalog/schemas

# It is assumed the workspace admin has set up IAM mapping or Unity Catalog
try:
    my_user = DA.username  # for grants, use user's email
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `{my_user}`")
    for schema in schemas:
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema} TO `{my_user}`")
        spark.sql(f"GRANT SELECT ON SCHEMA {catalog_name}.{schema} TO `{my_user}`")
except Exception as e:
    print(f"⚠️ Could not assign privileges: {e}")