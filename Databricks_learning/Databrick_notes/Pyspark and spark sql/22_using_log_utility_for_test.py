# Databricks notebook source
# DBTITLE 1,Invoking the custom log utility to get the log variables
# MAGIC
# MAGIC
# MAGIC %run ./21_custom_logging_logic

# COMMAND ----------

# DBTITLE 1,Test the log utility

ct_logger.info('----------reading the input file----')

try:
    df=spark.read.format('csv').option('header',True).option('inferSchema',True).load('/FileStore/tables/EmployeeAttrition/TestFile/WA_Fn_UseC__HR_Employee_Attrition1.csv')

except Exception as e:
    ct_logger.error(f'error_message : {e}')

# COMMAND ----------

# DBTITLE 1,Move the log file from tmp folder to dbfs location
# moving custom log files into dbfs log folder location
print(finalFileName)
print(partitions)
print(fileName)

dbutils.fs.mv('file:'+finalFileName , f'dbfs:/FileStore/tables/Custom_logs/{partitions}/{fileName}')

# COMMAND ----------

# DBTITLE 1,create a databse
# MAGIC %sql
# MAGIC
# MAGIC create database if not exists db

# COMMAND ----------

# DBTITLE 1,create a external table to read the logs
# MAGIC %sql
# MAGIC
# MAGIC drop table db.custom_logger_tble;
# MAGIC
# MAGIC create table if not exists db.custom_logger_tble
# MAGIC using text
# MAGIC location '/FileStore/tables/Custom_logs/*/*.log'

# COMMAND ----------

# DBTITLE 1,Reading custom log table
# MAGIC %sql
# MAGIC select * from db.custom_logger_tble

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC refresh table db.custom_logger_tble

# COMMAND ----------

#  removing the file handlers after notbook execution
# ct_logger.removeHandler(file_Handler) <--------------------------Add this line

