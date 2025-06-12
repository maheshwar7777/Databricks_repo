# Databricks notebook source
# MAGIC %md
# MAGIC Collect list and collect set are agg functions
# MAGIC
# MAGIC Collect list and Collect set : hear we  agg values into list. 
# MAGIC
# MAGIC But main difference is in collect list - we dont remove duplicate value in list (not removed the duplicate), whereas in collect set - we remove the duplicate value in agg list

# COMMAND ----------

# Create data frame and do group by
from pyspark.sql.functions import *

data = [(1,'Ram',34, 'IT'),(1,'Ram',54,'IT'),(2,'Sam',76,'Sci'),(2,'Sam',43,'IT'),(3,'Mahesh',67,'IT')]

schema = ['ID','Name','Marks','Subject']
df = spark.createDataFrame(data,schema
                            )

display(df)

# COMMAND ----------

# Using collect list

df_1=df.groupby("ID","Name").agg(collect_list(col("Subject")).alias('Sub'),sum("Marks").alias('Total_marks'))
display(df_1)

# COMMAND ----------

# Using collect set

df_1=df.groupby("ID","Name").agg(collect_set(col("Subject")).alias('Sub'),sum("Marks").alias('Total_marks'))
display(df_1)
