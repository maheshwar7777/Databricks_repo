# Databricks notebook source
for i in range(20):
    if i%2==0:
        print(i)

# COMMAND ----------

# MAGIC %sql
# MAGIC select curdate()

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/databricks-datasets/'))

# COMMAND ----------

display(dbutils.fs.help())

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kasireddymahesh7777@gmail.com/'))

#dbfs:/FileStore/shared_uploads/kasireddymahesh7777@gmail.com/first.csv

# COMMAND ----------

#display(dbutils.fs.ls('dbfs:/input/'))

#dbutils.fs.cp('dbfs:/FileStore/shared_uploads/kasireddymahesh7777@gmail.com/first.csv','dbfs:/input/')
df=spark.read.option('header',True).csv('dbfs:/input/first.csv')
display(df)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.widgets.help())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create Widget DropDown CountryDB Default 'india' Choices
# MAGIC select Country from 
# MAGIC (select 'india' as country union all select 'uk' as country) country

# COMMAND ----------

dbutils.widgets.remove('country DB')   #dropdown('country DB','india',{'india','uk'}, 'countryDB')
