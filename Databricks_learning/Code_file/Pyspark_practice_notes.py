# Databricks notebook source
# MAGIC %md
# MAGIC In the Document , we will cover below
# MAGIC
# MAGIC 1-> PySpark Dataframe
# MAGIC 2-> Reading the Dataset
# MAGIC 3->Checking the Datatypes of the columns(Schema)
# MAGIC 4->Selecting columns and indexing
# MAGIC 5->Check describe option similar to pandas
# MAGIC 6->Adding columns
# MAGIC 7->Droppping columns
# MAGIC 8-> Renaming the column names
# MAGIC
# MAGIC Test file path : dbfs:/FileStore/shared_uploads/kasireddymahesh7777@gmail.com/first.csv"
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Practice').getOrCreate()
spark

# COMMAND ----------

# MAGIC %md
# MAGIC In the documnet we will cover
# MAGIC
# MAGIC 1-> PySpark Dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("DataFrames").getOrCreate()
spark

# COMMAND ----------

##read the dataset
df=spark.read.option('header','true').csv('dbfs:/FileStore/shared_uploads/kasireddymahesh7777@gmail.com/first.csv',inferSchema=True)
df.show()

# COMMAND ----------

## check the schema
df.printSchema()

# COMMAND ----------

## another way to read the file
df_1=spark.read.csv('dbfs:/FileStore/shared_uploads/kasireddymahesh7777@gmail.com/first.csv',header=True,inferSchema=True)
df_1.show()
df_1.printSchema()

# COMMAND ----------

# Check the type of the variable
type(df_1)

# COMMAND ----------

# check the column names
df_1.columns

# COMMAND ----------

# pick some head elements

df_1.head(4)

## select a columns
df_1.select('Name').show()

##select multiple cloumns
df_1.select(['Name','salary']).show()

## check the datatypes
df_1.dtypes

## describe the objects
df_1.describe().show()

# COMMAND ----------

## Adding the columns in daatframes

df_2 = df_1.withColumn('Sal aft 2 years',df_1['salary']+100)

df_2.show()

# COMMAND ----------

## drop the columns

df_1 = df_2.drop('Sal aft 2 years')

df_1.show()

# COMMAND ----------

## Rename the columns

df_1.withColumnRenamed('Name','New Names').show()
