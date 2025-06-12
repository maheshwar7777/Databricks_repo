# Databricks notebook source
# Read csv file and drop column and drop duplicate rows
from pyspark.sql.functions import *
df=spark.read.option('header',True).option('inferSchema',True).csv('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

display(df)

# COMMAND ----------

# Drop single column

df_1=df.drop(col('Age'))
df_1.show()

df.show()

# COMMAND ----------

# Drop multi column

df_2=df.drop('Age','DOB')
df_2.show()



# COMMAND ----------

# Drop duplicate row in data frames

data = [(1,'Rram'),(2,'Sam'),(3,'Bhim'),(3,'Bhim'),(4,'Bhim')]

schema = ['ID','Name']
df3 = spark.createDataFrame(data,schema
                            )

df3.show()

# COMMAND ----------



df3.dropDuplicates().show()

# help(df3.dropDuplicates)

# COMMAND ----------

# Remove duplicate on specified column ( If it it multiple column, we should put them in list)
df3.dropDuplicates(['Name']).show()
