-- Databricks notebook source
-- MAGIC %python
-- MAGIC df=spark.read.option('header',True).csv('dbfs:/input/first.csv')
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df.createOrReplaceTempView('Salary_info')

-- COMMAND ----------

select * from Salary_info
