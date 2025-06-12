# Databricks notebook source
# Read csv file and case when

df=spark.read.option('header',True).option('inferSchema',True).csv('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

display(df)

df.collect()


# COMMAND ----------

# using when in withColumn method
from pyspark.sql.functions import when, col, lit

df.withColumn(
    "Voting",
    when(col("Age") > 18, lit("Eligible for voting")).otherwise("Not Eligible"),
).show()

# COMMAND ----------

# using when in select method
from pyspark.sql.functions import when, col, lit

df.select(
    "*",
    when(col("Age") > 18, lit("Eligible for voting")).otherwise("Not Eligible").alias('Voting')
).show()

# COMMAND ----------

# using multiple condition in when block in withColumn method
from pyspark.sql.functions import when, col, lit

df.withColumn(
    "Voting",
    when((col("Age") >= 30) & (col('Age')<25), lit("Eligible for voting")).otherwise("Not Eligible"),
).show()

# COMMAND ----------

# using multiple condition in when block in select method
from pyspark.sql.functions import when, col, lit

df.select(
    "*",
    when((col("Age") >= 30) & (col('Age')<25), lit("Eligible for voting")).otherwise("Not Eligible").alias('Voting')
).show()

# COMMAND ----------

# Using selectExpr method (In these methed , we can write sql queries)

df.selectExpr('*',"case when Age>30 and Age<25 then 'Eligible for voting' else 'Not Eligible' end as Voting").show()
