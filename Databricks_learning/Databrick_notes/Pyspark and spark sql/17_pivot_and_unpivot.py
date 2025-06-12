# Databricks notebook source
# creating dataframe

data = [('KTM','Q1',3000),
        ('KTM','Q2',2000),
        ('KTM','Q3',1000),
        ('Bajaj','Q1',1000),
        ('Bajaj','Q2',500),
        ('Bajaj','Q3',5000),
        ('Hero','Q1',1000),
        ('Hero','Q2',100),
        ('Hero','Q3',20000)
        ]

schema = ['Company','Quater','Revenue']

df1= spark.createDataFrame(data, schema)

display(df1)

# COMMAND ----------

#pivot the dataframe
from pyspark.sql.functions import *

df2 = df1.groupby('Company').pivot('Quater').sum('Revenue')

display(df2)

# COMMAND ----------

#unpivot the dataframe

df3_unpivot = df2.selectExpr("Company","stack(3,'Q1',Q1,'Q2',Q2,'Q3',Q3) as (Quater,Revenue)")

display(df3_unpivot)
