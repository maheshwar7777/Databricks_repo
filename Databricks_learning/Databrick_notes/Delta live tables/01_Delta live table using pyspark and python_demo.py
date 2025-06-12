# Databricks notebook source
"""
Delta live tables: we have three type of dlt tables

1.Streaming tables
2. Materialize view
3. View

"""

# COMMAND ----------

# To create a dlt materilized view
#  We can create dlt table only in premium and having unity catalog.
#  following one, we call it annotation function, hear also we can mention table name like  @dlt.table(name='<table name>')

@dlt.table
def <table_name>():
    df=spark.read.option('header',True).option('inferSchema',True).csv('dbfs:/FileStore/tables/DBFiles/raw_data.csv')
    return df


# hear the table will get created.

# COMMAND ----------

# To create a dlt streaming table

@dlt.table
def <table_name>():
    df = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet').option('cloudFiles.schemaLocation','<Provide a location to stores the schemaCheckPoint file(like provide volumn path in catalog)>').load('<provide input file path>')
    return df

#  hear streaming table will created.

# COMMAND ----------

# To create a dlt view from materialized view

@dlt.view
def <view_name>():
    df=dlt.read('<Materilized view name>')
    return df

# COMMAND ----------

# To create a dlt view from streaming view

@dlt.view
def <view_name>():
    df=dlt.readStream('<streaming table name>')
    return df
