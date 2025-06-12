# Databricks notebook source
"""
In Managed table, we create table and automatically the data is created in one location with path. Once we drop the table, the data in table gets erased and location path file also get erased.


Whereas

In External table, we create table and we specify the path which the data is created in one location with path. Once we drop the table, the data in table gets erased but location path file will not get erased.

"""

# COMMAND ----------

# Managed tables

# Read csv file for manipulation

df=spark.read.option('header',True).option('inferSchema',True).csv('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

display(df)

# COMMAND ----------

# Writing into a table 

df.write.mode('overwrite').saveAsTable('db.df_mngd_tble')

# COMMAND ----------

# MAGIC %sql
# MAGIC --desc the table
# MAGIC
# MAGIC desc extended db.df_mngd_tble

# COMMAND ----------

# Check the managed path file creation 

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/db.db/df_mngd_tble'))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --drop the table and check the table and file path
# MAGIC
# MAGIC drop table db.df_mngd_tble

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC desc extended db.df_mngd_tble

# COMMAND ----------

"""
Create External table
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create external table  db.df_ext_tble_1
# MAGIC (
# MAGIC   ID int
# MAGIC   ,Name string
# MAGIC   ,Age int
# MAGIC   ,DOB date
# MAGIC )
# MAGIC location 'dbfs:/FileStore/tables/ExternalTable/'

# COMMAND ----------

# Wirte the table into external table

df.write.mode('overwrite').saveAsTable('db.df_ext_tble_1')

# COMMAND ----------

# MAGIC %sql
# MAGIC --desc the table
# MAGIC
# MAGIC desc extended db.df_ext_tble_1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --drop the table and check the table and file path
# MAGIC
# MAGIC drop table db.df_ext_tble_1

# COMMAND ----------

# Check the external path file creation 

display(dbutils.fs.ls('dbfs:/FileStore/tables/ExternalTable'))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --access the external path file even though table gets dropped
# MAGIC
# MAGIC select * from delta.`dbfs:/FileStore/tables/ExternalTable`

# COMMAND ----------

#  Another way to create external table using pyspark

df.write.mode('overwrite').option('path','dbfs:/FileStore/tables/ExternalTable/Table2').saveAsTable('db.df_ext_tble_2')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from db.df_ext_tble_2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc extended db.df_ext_tble_2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- drop the external table 2
# MAGIC
# MAGIC drop table db.df_ext_tble_2

# COMMAND ----------

# Even though we drop the external table, the file still present in external location. mananully we need to delete the file

dbutils.fs.rm('dbfs:/FileStore/tables/ExternalTable/Table2',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta.`dbfs:/FileStore/tables/ExternalTable/Table2`
