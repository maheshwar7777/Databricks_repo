# Databricks notebook source
# MAGIC %md
# MAGIC #Explode :
# MAGIC     
# MAGIC Using explode funcion, we  divide  grouped data. It is opposite functionality of collect_list/Collect_set 
# MAGIC
# MAGIC ## type of explode function:
# MAGIC
# MAGIC   1-->Explode  : here explode breakdown (array/ map and value) into new row and exclude the null values
# MAGIC
# MAGIC   2-> explode_outer : here explode_outer breakdown (array/ map and value) into new row and includes the null values
# MAGIC
# MAGIC
# MAGIC   3-> posexplode : It is same as explode, and additionally, a new column would get added with position of (array/ map and value) value 
# MAGIC
# MAGIC   4-> posexplode_outer  : It is same as explode, and additionally, a new column would get added with position of (array/ map and value) value 

# COMMAND ----------

# Create data frame and do group by
from pyspark.sql.functions import *

data = [(1,'Ram',34, 'IT'),(1,'Ram',54,'Maths'),(2,'Sam',76,'Sci'),(2,'Sam',43,'IT'),(3,'Mahesh',67,'IT')]

schema = ['ID','Name','Marks','Subject']
df = spark.createDataFrame(data,schema
                            )

df_1=df.groupby("ID","Name").agg(collect_list(col("Subject")).alias('Sub'),sum("Marks").alias('Total_marks'))
display(df_1)

# COMMAND ----------

# Here using explode funtion , we are diving grouped(in list) data. (here we converting one row into mulitple row)

df_1.select("ID","Name",explode("Sub").alias("Subjects")).show()
