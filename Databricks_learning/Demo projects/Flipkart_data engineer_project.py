# Databricks notebook source
# MAGIC %md
# MAGIC # Create spark session (note : by default spark session will be on in databricks notebook)
# MAGIC
# MAGIC  -->we can use these command in jupiter notebook

# COMMAND ----------

#import the libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#create spark session 

spark=SparkSession.builder.appName('Flipkart project').getOrCreate()



# COMMAND ----------

# get the input file path

display(dbutils.fs.ls('/FileStore/tables/FlipkartProject/testFile'))

filePath='/FileStore/tables/FlipkartProject/testFile/Flipkart.csv'



# COMMAND ----------

#ready the input csv file

df_inputData=spark.read.format('csv').option('header',True).option('inferSchema',True).load(filePath)

display(df_inputData)

# COMMAND ----------

# check the column names and datatypes

df_inputData.columns

# COMMAND ----------

#checking the schema

display(df_inputData.printSchema())

# COMMAND ----------

# Handle missing data

df_inputData.select( [count( when( col(c).isNull() , c ) ).alias(c)  for c in df_inputData.columns] ).display()

# COMMAND ----------

#drop the null value rows and stored the result set in separate dataframes for analysis

flp_df_dp_null_val = df_inputData.dropna()

#fill the missing  values to the nan column or missing columns

flp_df_filling_val = df_inputData.fillna({"Rating":0,"maincateg":"No Sex","star_5f":0,"star_1f":0}) 

# COMMAND ----------

#data transformation

#calculate effective price after discount

flp_df_col_add = flp_df_filling_val.withColumn("Discount",lit(20))

flp_df_transfm = flp_df_col_add.withColumn("effectivePrice",expr("actprice1 - (actprice1 * (Discount/100) )"))

#rename the column name

flp_df_transfm_new = flp_df_transfm.withColumnRenamed('actprice1','Price')



# COMMAND ----------

#show the update dataframes

flp_df_transfm_new = flp_df_transfm_new.select(flp_df_transfm_new.title,flp_df_transfm_new.Price,flp_df_transfm_new.effectivePrice,flp_df_transfm_new.Discount).display()

# COMMAND ----------

# drop the duplicate row if there any

flp_df_transfm_new = flp_df_transfm_new.dropDuplicates()

# COMMAND ----------

#  Filter the tile / items with rating greater than 4

high_rating_prodcut = flp_df_transfm_new.filter(col('Rating')>4)

high_rating_prodcut.display()

# COMMAND ----------

# Group by the gender and calculate the avg rating

Avg_rating_by_gender = flp_df_transfm_new.groupBy(col('maincateg').alias('Gender')).agg(avg('Rating').alias('Avg_rating'))

Avg_rating_by_gender.display()

# COMMAND ----------

# Total revenue by gender

tot_rev_by_Gender = flp_df_transfm_new.groupBy(col('maincateg').alias('Gender')).agg(round(sum('effectivePrice')).alias('Total_revenue'))

tot_rev_by_Gender.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC --create  database to store the data in delta table
# MAGIC
# MAGIC create database if not exists Flipkt_DB
# MAGIC

# COMMAND ----------

#Save the transformed data in delta table

flp_df_transfm_new.write.format('delta').mode('overwrite').saveAsTable('Flipkt_DB.Flipkt_transformed_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Flipkt_DB.Flipkt_transformed_data limit 20
