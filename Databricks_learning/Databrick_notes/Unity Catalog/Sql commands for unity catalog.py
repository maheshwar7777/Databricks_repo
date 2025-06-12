# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC --create catalog in databricks ( the user must have access to create a catalog)
# MAGIC
# MAGIC create catalog if not exists test_dev;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Before creating schema (we must use created catalog) or mention catalog name bfore schema name (like test_dev.bronze)
# MAGIC
# MAGIC use catalog test_dev;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- create schema inside catalog
# MAGIC
# MAGIC create schema if not exists  bronze;
# MAGIC create schema if not exists  silver;
# MAGIC create schema if not exists  gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --use schema name before creating tables or mention catalog name, schema name before table name
# MAGIC
# MAGIC use schema bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --create a table
# MAGIC
# MAGIC create or replace table bronze_raw_tbl 
# MAGIC (
# MAGIC ID int
# MAGIC ,Name string
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --to show the catalogs
# MAGIC
# MAGIC show catalogs ;
# MAGIC
# MAGIC --to show schema 
# MAGIC
# MAGIC show schemas;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- to grant create table access to user
# MAGIC
# MAGIC grant CREATE TABLE on schema 'schema name (like test_dev.bronze )' to 'user mail id or group or service principle'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --to show the user who got the grant access
# MAGIC
# MAGIC show grants 'user mail id' on  schema 'schema'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- check the revoke grrant access 
