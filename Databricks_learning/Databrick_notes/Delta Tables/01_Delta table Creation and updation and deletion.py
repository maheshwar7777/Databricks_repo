# Databricks notebook source
# create a dataframe

data = [(1,'Mahesh',3000,2000),(2,'Ramesh',4000,100),(3,'Suresh',2000,1000),(4,'Ram',30000,2000)]
schema = ['ID','Name','Salary','Bonus']

df=spark.createDataFrame(data,schema)

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Create database (note : in community verison , once cluster terminates, database gets dropped)
# MAGIC
# MAGIC create database if not exists db
# MAGIC
# MAGIC

# COMMAND ----------

# Create delta table using dataframes

df.write.mode('overwrite').saveAsTable('db.delta_temp_1')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc extended db.delta_temp_1

# COMMAND ----------

# check the table file creation path and it will have name as delta

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/db.db/delta_temp_1/_delta_log/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* creating delta table using sql */
# MAGIC
# MAGIC create table db.delta_tbl_2
# MAGIC (
# MAGIC     ID long
# MAGIC ,Name string
# MAGIC ,Salary long
# MAGIC ,Bonus long
# MAGIC ) using delta
# MAGIC
# MAGIC --location 'path' /* we need to mention the path to create external delta table */

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --insert data into delta table
# MAGIC
# MAGIC insert into db.delta_tbl_2  values(2,'Ramesh ',1000,300);
# MAGIC insert into db.delta_tbl_2  values(3,'Suresh',4000,500);
# MAGIC insert into db.delta_tbl_2  values(4,'Sam ',5000,200);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from db.delta_tbl_2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --updating delta table (note : we can make DML activities only to Delta tables)
# MAGIC
# MAGIC update db.delta_tbl_2 set Bonus=400 where id=2;
# MAGIC
# MAGIC

# COMMAND ----------

# Updating delta table using python

spark.sql('update db.delta_tbl_2 set Bonus=500 where id=2;')


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Deleting records */
# MAGIC
# MAGIC delete from db.delta_tbl_2 where id=2;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from db.delta_tbl_2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --truncating table
# MAGIC
# MAGIC truncate table db.delta_tbl_2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from db.delta_tbl_2
