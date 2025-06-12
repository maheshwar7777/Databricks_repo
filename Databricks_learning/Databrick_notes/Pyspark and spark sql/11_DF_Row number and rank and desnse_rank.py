# Databricks notebook source
# MAGIC %md
# MAGIC # Row number and rank and dense_rank

# COMMAND ----------

from pyspark.sql.functions import row_number,rank,dense_rank,col,lit
from pyspark.sql.window import Window

data = [(1,'Mahesh','IT'),(1,'Mahesh','Math'),(1,'Reddy','Math'),(1,'Jai','Math'),(2,'Ram','Math'),(3,'Sam','IT'),(3,'Jam','Sci')]
schema = ['ID','Name','Sub']

df=spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

# using row_number

wind_spec = Window.partitionBy(col('ID')).orderBy(col('ID'),col('Name'))
df_final = df.withColumn('rn',row_number().over(wind_spec))

display(df_final)

# COMMAND ----------

# using rank function

wind_spec = Window.partitionBy(col('ID')).orderBy(col('Name'))
df_final = df.withColumn('rnk',rank().over(wind_spec))

display(df_final)

# COMMAND ----------

# using dense_rank function

wind_spec = Window.partitionBy(col('ID')).orderBy(col('Name'))
df_final = df.withColumn('dense_rnk',dense_rank().over(wind_spec))

display(df_final)
