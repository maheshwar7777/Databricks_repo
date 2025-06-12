# Databricks notebook source
# Create data frame and do group by
from pyspark.sql.functions import *

data = [(1,'Ram',34, 'IT'),(1,'Ram',54,'Maths'),(2,'Sam',76,'Sci'),(2,'Sam',43,'IT'),(3,'Mahesh',67,'IT')]

schema = ['ID','Name','Marks','Subject']
df = spark.createDataFrame(data,schema
                            )

display(df)

# COMMAND ----------

df_1 = df.select('*',create_map(col('Name'),lit('Delhi'),lit('country'),lit('india')).alias('map'))

display(df_1)

# COMMAND ----------

df_1.select('*',explode('map')).show()

# to get specific key using getItem() funciton
df_1.select('*',col('map').getItem('country')).show()
