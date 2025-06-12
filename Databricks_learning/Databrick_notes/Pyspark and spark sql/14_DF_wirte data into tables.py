# Databricks notebook source
# Read csv file 

df=spark.read.option('header',True).option('inferSchema',True).csv('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

display(df)

# COMMAND ----------

'''
 Create database to write the data into table

 we have below mode to write to table
    overwrite
    append
    ignore
    error

command to wirte to the table is :
   ==>  df.write.format('csv/parquet..').option('sep','|').mode('overwrite').save()   /or saveAsTable
    here save is used to save the file in filesystem
    whereas saveAsTable is used to save in table

'''

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --create a database
# MAGIC
# MAGIC create database db

# COMMAND ----------

# Write the data into table

df.write.format('csv').option('sep','|').mode('overwrite').saveAsTable('db.raw_data_input')

# COMMAND ----------

# Use below command to remove the files 

# dbutils.fs.rm('dbfs:/user/hive/warehouse/db.db/raw_data_input',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from db.raw_data_input

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select current_date(),month(current_date()),* from db.raw_data_input

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --describle the table 
# MAGIC
# MAGIC desc extended db.raw_data_input
# MAGIC
# MAGIC /*  another syntax : desc table extended <table_name>   */

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/TestDfToFile/'))

# COMMAND ----------

# Write dataframe data into file 

df.write.format('csv').option('sep','|').mode('overwrite').save('dbfs:/FileStore/tables/TestDfToFile')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --access save filed in sql
# MAGIC
# MAGIC select * from csv.`dbfs:/FileStore/tables/TestDfToFile`

# COMMAND ----------

''''
Write data into file using partitionby method

''''

# COMMAND ----------

# Create data frame and do group by
from pyspark.sql.functions import *

data = [(1,'Ram',34, 'IT'),(1,'Ram',54,'IT'),(2,'Sam',76,'Sci'),(2,'Sam',43,'IT'),(3,'Mahesh',67,'IT'),(3,'Mahesh',67,'Sci'),(4,'Bam',67,'Math'),(4,'Bam',67,'Sci'),(5,'Krish',67,'IT')]

schema = ['ID','Name','Marks','Subject']
df = spark.createDataFrame(data,schema
                            )

display(df)

# COMMAND ----------

# wite to file

df.write.partitionBy('Subject').format('parquet').mode('overwrite').save('dbfs:/FileStore/tables/TestDfToFile/Partitionby/')

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/TestDfToFile/Partitionby/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --query only respected parition folder (here like suject IT or Math or Sci)
# MAGIC
# MAGIC select * from parquet.`dbfs:/FileStore/tables/TestDfToFile/Partitionby/Subject=Math/`;
# MAGIC
# MAGIC --to acess all the partition folder
# MAGIC select * from parquet.`dbfs:/FileStore/tables/TestDfToFile/Partitionby/`

# COMMAND ----------

# check whether adaptive query execution is enabled

spark.conf.get('spark.sql.adaptive.enabled')
