# Databricks notebook source
# Read csv file for date manipulation

df=spark.read.option('header',True).option('inferSchema',True).csv('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

display(df)

# COMMAND ----------

# create new column with current date

from pyspark.sql.functions import current_date,current_timestamp,year,month,day

df.withColumn('Current_date',current_date()).show()

# COMMAND ----------

# create new column with current date , year and current_timestamp

from pyspark.sql.functions import current_date,current_timestamp,year,month,day,col

df.withColumn('Current_date',current_date()).withColumn('Year',year(col('DOB'))).withColumn('Current_timestamp',current_timestamp()).show()

# COMMAND ----------

# create new column with current date , year and current_timestamp

from pyspark.sql.functions import current_date,current_timestamp,year,month,day,col,date_format,to_date

df.withColumn('New_date_format',date_format(col('DOB'),format='yyyy/MM/dd')).show()

df.withColumn('New_date_format',date_format(col('DOB'),format='yyyy-MM/dd')).show()

df.withColumn('New_date_format',date_format(col('DOB'),format='yyyy/MM-dd')).show()

df.withColumn('New_date_format',date_format(col('DOB'),format='yyyy/MMM-dd')).show()
