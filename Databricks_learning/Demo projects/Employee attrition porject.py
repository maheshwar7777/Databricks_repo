# Databricks notebook source
# MAGIC %md
# MAGIC # Employee attrition porject

# COMMAND ----------

'''
Employee attrition problem
Employees are the backbone of the organization. Organization's performance is heavily based on the quality of the employees. Challenges that an organization has to face due employee attrition are:
1. Expensive in terms of both money and time to train new employees.
2. Loss of experienced employees
3. Impact in productivity
4. Impact profit
Business questions to brainstorm:
1. What factors are contributing more to employee attrition?

2. What type of measures should the company take in order to retain their employees?

'''

# COMMAND ----------

# move the test file from onle place to another
# command to copy the file
# dbutils.fs.cp('/FileStore/tables/WA_Fn_UseC__HR_Employee_Attrition.csv','/FileStore/tables/EmployeeAttrition/TestFile/WA_Fn_UseC__HR_Employee_Attrition.csv')

#check if the file exist in destination folder

display(dbutils.fs.ls('/FileStore/tables/EmployeeAttrition/TestFile/WA_Fn_UseC__HR_Employee_Attrition.csv'))




# COMMAND ----------

# read input file into dataframes

filePath = '/FileStore/tables/EmployeeAttrition/TestFile/WA_Fn_UseC__HR_Employee_Attrition.csv'

df = spark.read.format('csv').option('header',True).option('inferSchema',True).load(filePath)

# df.cache()
display(df)



# COMMAND ----------

# check the columns of dataframe

df.columns

display(df.count())

# COMMAND ----------

display(dbutils.data.summarize(df))

# COMMAND ----------

# Get the null/blanks of columns in dataframe
from pyspark.sql.functions import *
df_nulls_count = df.select( [count(when( col(c).isNull(), c )).alias(c)      for c in df.columns]  )

display(df_nulls_count)

# COMMAND ----------

# drop the null records and stored it in separate dataframe

# df_null_records = df.dropna()

df.dropna(how="all").display()    #it correct result, it showing the result set after dropping all null value rows if there are any.

# display(df_null_records)

# COMMAND ----------

df.rdd.getNumPartitions()  # to get the parition number for dataframe

df.rdd.glom().collect()  # to check on number of partitions created in data

# COMMAND ----------


df_01 = df.selectExpr('spark_partition_id() as spark_part_id', '*')

df_01.groupby(col('spark_part_id')).count().display()

# df_01.display()

# df.unpersist()

# COMMAND ----------

# MAGIC %sql
# MAGIC /*  create databse  for delta table */
# MAGIC
# MAGIC
# MAGIC create database if not exists bronze;
# MAGIC

# COMMAND ----------

# create a delta table

df.write.format('delta').mode('overwrite').saveAsTable('bronze.emp_attr_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.emp_attr_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Employee total count

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Get emp total count in sql */
# MAGIC
# MAGIC select sum(EmployeeCount) from bronze.emp_attr_data;

# COMMAND ----------

#  Get emp total count in pyspark
from pyspark.sql.functions import *
df.agg(sum('EmployeeCount').alias('total_emp_count')).show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Find the attrition count based on employee count

# COMMAND ----------

# MAGIC %sql
# MAGIC -- in sql
# MAGIC
# MAGIC select Attrition, sum(EmployeeCount)  as total_emp_count from  bronze.emp_attr_data group by Attrition;

# COMMAND ----------

# in pyspark

df.groupby(col('Attrition')).agg(sum('EmployeeCount').alias('total_emp_count')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### AGE Analysis --> lets find out which particular aqe attrition is high (18-24, 25-31,32-38,39-45,46-52,52+)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- in sql
# MAGIC
# MAGIC select sum(EmployeeCount) as total_emp_count
# MAGIC ,case when age between 18 and  25 then '18-25'
# MAGIC       when age between 26 and  40 then '26-40'
# MAGIC       when age between 41 and  55 then '41-55'
# MAGIC       when age > 55 then '55+'
# MAGIC
# MAGIC end as age_group
# MAGIC
# MAGIC from bronze.emp_attr_data where attrition = 'Yes' 
# MAGIC group by case when age between 18 and  25 then '18-25'
# MAGIC       when age between 26 and  40 then '26-40'
# MAGIC       when age between 41 and  55 then '41-55'
# MAGIC       when age > 55 then '55+' end 
# MAGIC order by 2;
# MAGIC
# MAGIC

# COMMAND ----------

# in pyspark

df_1 = df.filter(col('attrition')=='Yes').groupby(col('age')).agg(sum('EmployeeCount').alias('total_emp_count'))

# df_2 = df_1.select( when( (col('age')>=18) & (col('age')<=25) ,'15-25').when( (col('age')>=26) & (col('age')<=40) ,'26-40') \
#                     .when( (col('age')>=41) & (col('age')<=55) ,'41-55') \
#                     .when( col('age')>55 ,'56+').alias('age_group'), 'total_emp_count')

df_2 = df_1.selectExpr("case when age between 18 and 25 then '18-25' \
                when age between 26 and  40 then '26-40' \
      when age between 41 and  55 then '41-55' \
      when age > 55 then '55+' end as age_group " , "total_emp_count")

df_2.groupby(col('age_group')).agg(sum('total_emp_count')).show()



# COMMAND ----------

# MAGIC %md
# MAGIC ###  FIND OUT Attrition by Department

# COMMAND ----------

# MAGIC %sql
# MAGIC -- in sql
# MAGIC
# MAGIC select sum(EmployeeCount) as total_emp_count
# MAGIC ,Department
# MAGIC
# MAGIC from bronze.emp_attr_data where attrition = 'Yes' 
# MAGIC group by Department 
# MAGIC order by 2;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# in pyspark

df.filter(col('attrition')=='Yes').groupby(col('Department')).agg(sum('EmployeeCount').alias('total_emp_count')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### attrition by education - ( 1- below college, 2 - college , 3 bachelor , 4 master, S doctor)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- in sql
# MAGIC select sum(EmployeeCount) as total_emp_count
# MAGIC ,case when Education = 1 then 'below college'
# MAGIC       when Education = 2 then 'college'
# MAGIC       when Education = 3 then 'bachelor'
# MAGIC       when Education = 4 then 'masters'
# MAGIC       when Education = 5 then 'doctor'
# MAGIC   end as Education_sts
# MAGIC
# MAGIC from bronze.emp_attr_data where attrition = 'Yes' 
# MAGIC group by case when Education = 1 then 'below college'
# MAGIC       when Education = 2 then 'college'
# MAGIC       when Education = 3 then 'bachelor'
# MAGIC       when Education = 4 then 'masters'
# MAGIC       when Education = 5 then 'doctor'
# MAGIC   end 
# MAGIC order by 2;
# MAGIC
