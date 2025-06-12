# Databricks notebook source
"""
UDF -- User defined function

We can write funtions in python and register in pyspark and use it in pyspark code

"""

# COMMAND ----------

# create a dataframe

data = [(1,'Mahesh',3000,2000),(2,'Ramesh',4000,100),(3,'Suresh',2000,1000),(4,'Ram',30000,2000)]
schema = ['ID','Name','Salary','Bonus']

df=spark.createDataFrame(data,schema)

display(df)

# COMMAND ----------

# Write a python funtion to return the total pay

def TotalPay(x,y):
    return x+y

# Now registor the python function in pyspark

from pyspark.sql.functions import udf,col
from pyspark.sql.types import IntegerType

totalPay = udf(lambda x,y : TotalPay(x,y),IntegerType())

# Now use the UDF funtion in pyspark code

df.withColumn('Total_Salary',totalPay(df.Salary,df.Bonus)).show()

# COMMAND ----------

# Another way to register python funtion in udf 

from pyspark.sql.functions import udf,col
from pyspark.sql.types import IntegerType

@udf(returnType=IntegerType())
def TotalPay(x,y):
    return x+y
    
# Now use the UDF funtion in pyspark code

df.withColumn('Total_Salary',TotalPay(df.Salary,df.Bonus)).show()

# COMMAND ----------

"""
We can registr udf in sql too

"""

# COMMAND ----------

# create a dataframe

data = [(1,'Mahesh',3000,2000),(2,'Ramesh',4000,100),(3,'Suresh',2000,1000),(4,'Ram',30000,2000)]
schema = ['ID','Name','Salary','Bonus']

df=spark.createDataFrame(data,schema)

df.createOrReplaceTempView('tempV')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from tempV

# COMMAND ----------

# Another way to register  funtion in sql

from pyspark.sql.functions import udf,col
from pyspark.sql.types import IntegerType


def TotalPay(x,y):
    return x+y

spark.udf.register(name='totalPay', f=TotalPay, returnType=IntegerType())

# COMMAND ----------

# MAGIC %sql 
# MAGIC --use the udf function in sql
# MAGIC
# MAGIC select * , totalPay(Salary,bonus) as total_salary from tempV
