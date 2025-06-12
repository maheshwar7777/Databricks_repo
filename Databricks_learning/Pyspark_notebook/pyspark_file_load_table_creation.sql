-- Databricks notebook source
-- MAGIC %python
-- MAGIC # display(dbutils.fs.ls('dbfs:/input/first.csv'))
-- MAGIC
-- MAGIC df=spark.read.option('header',True).csv('dbfs:/input/first.csv')
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df.schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #create dataframes with new datatype for csv data 
-- MAGIC
-- MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType
-- MAGIC
-- MAGIC schema_new = StructType([
-- MAGIC                 StructField('id',IntegerType(),True),
-- MAGIC                 StructField('Name',StringType(),True),
-- MAGIC                 StructField('Salary',IntegerType(),True),
-- MAGIC
-- MAGIC ])
-- MAGIC
-- MAGIC df_new = spark.read.option('header',True).schema(schema_new).csv('dbfs:/input/first.csv')
-- MAGIC
-- MAGIC display(df_new)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df_new.schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df_new.write.csv('dbfs:/output/first.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC  display(dbutils.fs.ls('dbfs:/output'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col,lit 
-- MAGIC
-- MAGIC df_new_1 = df_new.withColumn('Country',lit('india'))
-- MAGIC
-- MAGIC df_new_1.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=df_new_1.filter(df_new_1.Name=="Ram")
-- MAGIC display(df1)
-- MAGIC
-- MAGIC df1.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import *
-- MAGIC df_new_1.select('Name',df_new_1.Salary).show()
-- MAGIC
