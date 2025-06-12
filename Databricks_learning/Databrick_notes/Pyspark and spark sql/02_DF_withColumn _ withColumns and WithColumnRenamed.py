# Databricks notebook source
# Read csv file and add cloumns and modify the columns

df=spark.read.option('header',True).option('inferSchema',True).csv('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

df.withColumn('Flag',lit('Y')).show()

# COMMAND ----------

# adding multiple colunms using withcolumn method

from pyspark.sql.functions import *

df.withColumn('Flag',lit('Y')).withColumn('Id_New',col('ID')+1).show()

# COMMAND ----------

# adding multiple colunms using new withColumns method

from pyspark.sql.functions import *

df.withColumns({'Flag': lit('Y'),'Id_New': col('ID')+1}).show()

# COMMAND ----------

# MAGIC %md
# MAGIC WithColumnRenamed method
# MAGIC

# COMMAND ----------

# Rename single column

df.withColumnRenamed('ID','ID_New').show()

# COMMAND ----------

# Rename mulitple columns

df.withColumnsRenamed({'ID':'ID_New','DOB':'Date of Birth'}).show()

# COMMAND ----------

# MAGIC %md
# MAGIC We can rename column names using select method
# MAGIC

# COMMAND ----------

# Rename column name using select method

from pyspark.sql.functions import col

df.select(col('ID').alias('ID_New'),'*').drop('ID').show()

display(df)

# COMMAND ----------

# Rename column name using selectExpr method

df.selectExpr('ID as ID_New','*').drop('ID').show()
