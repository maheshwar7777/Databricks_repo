# Databricks notebook source
# load the file

df=spark.read.csv('dbfs:/FileStore/shared_uploads/kasireddymahesh7777@gmail.com/first.csv',header=True,inferSchema=True)

df.show()

# COMMAND ----------

df.createOrReplaceTempView("employee")

df1=spark.sql("select Name from employee")

df1.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select * from employee;
# MAGIC
# MAGIC select max(salary) from employee 
# MAGIC
# MAGIC
# MAGIC
