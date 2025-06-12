# Databricks notebook source
# Create data frame and do group by
from pyspark.sql.functions import *

data = [(1,'Ram',34, 'IT'),(1,'Ram',54,'Math'),(2,'Sam',76,'Sci'),(2,'Sam',43,'IT'),(3,'Mahesh',67,'IT')]

schema = ['ID','Name','Marks','Subject']
df = spark.createDataFrame(data,schema
                            )

df.show()
df.count()

# COMMAND ----------

# use groupby method

df.groupby('ID','Name').agg(sum('Marks').alias('Total_marks'),countDistinct('Subject').alias('Subject_counts')).show()

# using agg() funcion using dict
# type : 1
df.groupBy('ID','Name').agg(*{'Total_marks':sum('Marks'), 'Subject_counts': countDistinct('Subject')}.values()).show()

# type :2
df.groupBy('ID','Name').agg({'Marks':'sum'}).show()



# COMMAND ----------

# use groupby and where method

df.groupby('ID','Name').agg(sum('Marks').alias('Total_marks')).filter(col('Total_marks')>80).show()

# COMMAND ----------

# use groupby ,where and order by method

from pyspark.sql.functions import *

df_1 = df.groupby('ID','Name').agg(sum('Marks').alias('Total_marks')).filter(col('Total_marks')>80).orderBy('ID','Name')

df_1.explain()  
