# Databricks notebook source
# MAGIC %md
# MAGIC ##  High level info::
# MAGIC
# MAGIC & --- And operator
# MAGIC
# MAGIC | -- Or operator
# MAGIC
# MAGIC ~ -- Not operator
# MAGIC
# MAGIC #### other filter methods:
# MAGIC
# MAGIC 1-> isNull()
# MAGIC
# MAGIC 2-> isnotNull()
# MAGIC
# MAGIC 3-> == , >= , <=, > , <
# MAGIC
# MAGIC 4-> .endsWith()
# MAGIC
# MAGIC 5-> startsWith()
# MAGIC
# MAGIC 6-> contains()
# MAGIC
# MAGIC 7-> like(%%)
# MAGIC
# MAGIC 8-> isin()   --> here we can pass multiple values like (100,2000, 540)

# COMMAND ----------

# Read csv file and use filter concepts

df=spark.read.option('header',True).option('inferSchema',True).csv('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

display(df)

# COMMAND ----------


# Using like condition in filter method

from pyspark.sql.functions import col
df.filter(col('Name').like('%e%')).show()

# COMMAND ----------


# Using multiple condition in filter method

from pyspark.sql.functions import col
df.filter((col('Name').like('%S%')) & (col('Age')>18)).show()

# COMMAND ----------

# Using multiple condition in where method

from pyspark.sql.functions import col
df.where((col('Name').like('%S%')) & (col('Age')>18)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting Or Order by

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

df.orderBy((col('ID').desc()),(col('Name').asc())).show()
           

