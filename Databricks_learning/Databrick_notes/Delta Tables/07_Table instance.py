# Databricks notebook source
# MAGIC %md
# MAGIC # **Table Instance:**
# MAGIC
# MAGIC ###  suppose if in any project requirment, if they dont want to use delta tables using spark sql and want to use delta tables using pyspark, we can create delta tables using pyspark.

# COMMAND ----------

# synatax for create delta table using pyspark.

DeltaTable.create(spark)\
    .tableName('')\
    .addColumn('')\
    .addColumn('')\
    .addColumn('')\
    .property('description','Table created')\
    .location('provide table location')
    .execute()
    

# COMMAND ----------

# syntax to create table instance

# method 1:
from delta.table import *

table_inst = DeltaTable.forPath(spark,'provide the above table creation path')

or 

# method 2:
table_inst = DeltaTable.forName(spark,'table name')

# COMMAND ----------

# to access the table instance like table, use below one (here toDF mean converting table instance to dataFrame)

display(table_inst.toDF)
