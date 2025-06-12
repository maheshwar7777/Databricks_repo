# Databricks notebook source
# MAGIC %md
# MAGIC Using optimize, we can club multiple small file into one single file

# COMMAND ----------

df=spark.range(1,10000)

df.repartition(10).write.mode('overwrite').save('dbfs:/FileStore/tables/Optimize/')

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/Optimize/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --now use optimize option to club the file
# MAGIC
# MAGIC optimize delta.`dbfs:/FileStore/tables/Optimize/`

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/Optimize/'))

display(dbutils.fs.ls('dbfs:/FileStore/tables/Optimize/_delta_log/'))

# COMMAND ----------

# MAGIC %md
# MAGIC Optimize multiple columns 

# COMMAND ----------

# create a dataframe

data = [
    (1, "Mahesh", "Dev", 3000, 2000),
    (2, "Ramesh", "Test", 4000, 100),
    (3, "Suresh", "Dev", 2000, 1000),
    (4, "Ram", "PM", 30000, 2000),
    (5, "Vishnu", "PM", 30000, 2000),
    (6, "Hari", "Test", 30000, 2000),
    (7, "Hari", "Dev", 30000, 2000),
    (8, "Hari", "Dev", 30000, 2000),
    (9, "Hari", "Test", 30000, 2000),
    (10, "Hari", "Test", 30000, 2000),
]
schema = ["ID", "Name",'Dept', "Salary", "Bonus"]

df = spark.createDataFrame(data, schema)

display(df)

# COMMAND ----------

# optimize the mulitple file

df.repartition(3).write.partitionBy('Dept').mode('overwrite').save('dbfs:/FileStore/tables/Optimize_multipleCols/')

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/Optimize_multipleCols/Dept=Test/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --now use optimize option for onbe specific dept
# MAGIC
# MAGIC optimize delta.`/FileStore/tables/Optimize_multipleCols/` where Dept='Test'

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/Optimize_multipleCols/Dept=Test/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ####  z-ordering :

# COMMAND ----------

'''
What is Z- ordering :

Z-Ordering is a technique to colocate related information in the same set of files. This co-locality is automatically used by Delta Lake on Databricks data-skipping algorithms to dramatically reduce the amount of data that needs to be read. Syntax for Z-ordering can be found here.

If you expect a column to be commonly used in query predicates and if that column has high cardinality (that is, a large number of distinct values) which might make it ineffective for PARTITIONing the table by, then use ZORDER BY instead (ex:- a table containing companies, dates where you might want to partition by company and z-order by date assuming that table collects data for several years)

You can specify multiple columns for ZORDER BY as a comma-separated list. However, the effectiveness of the locality drops with each additional column.

'''

'''
In simpler defination : We use optimize techique to club multiple smalled file into single file. And on top of it.

Z-ordering : we use Z-ordering techique to club multiple smalled file into single file and sort the data based on column we provide.

'''

# COMMAND ----------

# MAGIC %sql
# MAGIC --command to apply z-order technique.
# MAGIC
# MAGIC optimize delta.`/FileStore/tables/Optimize_multipleCols/` where Dept='Test'
# MAGIC zorder by id
