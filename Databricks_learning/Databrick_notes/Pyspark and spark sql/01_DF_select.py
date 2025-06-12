# Databricks notebook source
# Read csv file for manipulation

df=spark.read.option('header',True).option('inferSchema',True).csv('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

df.select('ID','Name').show()

# COMMAND ----------

# df.columns

df.select(df.columns[:-2]).show()

# COMMAND ----------

from pyspark.sql.types import *

data = [(('Mahesh','Reddy'),'AP','M'),\
    (('Ram','krishna'),'KA','M'),\
        (('Priya','Chaitu'),'TL','F')\
        ]

FLNames = StructType(
    [
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
    ]
)

schema = StructType(
    [
        StructField("Names", FLNames, True),
        StructField("State", StringType(), True),
        StructField("Gender", StringType(), True),
    ]
)



#schema = ['Name','State','Gender']

df_1=spark.createDataFrame(data,schema)

df_1.show()

df_1.printSchema()

# COMMAND ----------

df_1.select(df.Names.firstName).show()

# COMMAND ----------

df_1.select('Names.*').show()

# COMMAND ----------

from pyspark.sql.functions import *

df_1.select(col('*'),explode(col('Names'))).show()
