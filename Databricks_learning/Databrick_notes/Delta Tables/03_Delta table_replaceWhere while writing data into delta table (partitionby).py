# Databricks notebook source
# MAGIC %md
# MAGIC  **Using partitionby** : we can save data in table / file based on paritioned row. we use these function when we want to update only partitioned row and not other. So it will update only those rows and not touching other row.

# COMMAND ----------

# create a dataframe

data = [
    (1, "Mahesh", "Dev", 3000, 2000),
    (2, "Ramesh", "Test", 4000, 100),
    (3, "Suresh", "Dev", 2000, 1000),
    (4, "Ram", "PM", 30000, 2000),
    (5, "Vishnu", "PM", 30000, 2000),
    (6, "Hari", "Test", 30000, 2000),
]
schema = ["ID", "Name",'Dept', "Salary", "Bonus"]

df = spark.createDataFrame(data, schema)

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --create database if not exists
# MAGIC
# MAGIC create database if not exists db

# COMMAND ----------

# write the table/file in partitioned manner

df.write.partitionBy('Dept').mode('overwrite').saveAsTable('db.delta_tbl_partition')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db.delta_tbl_partition

# COMMAND ----------

"""
If we want to update only specific types of records without losing old data when dataframe was updated, we have use replaceWhere option

"""

# COMMAND ----------

# create a dataframe

data = [
    (1, "Mahesh", "Dev", 8000, 2000),
    (3, "Suresh", "Dev", 8000, 1000),
]
schema = ["ID", "Name",'Dept', "Salary", "Bonus"]

df_1 = spark.createDataFrame(data, schema)

display(df_1)

# COMMAND ----------

# write the table with only latest dataframe data without losing old table and update only new dataframe data

df_1.write.partitionBy('Dept').mode('overwrite').option('replaceWhere',"Dept=='Dev'").saveAsTable('db.delta_tbl_partition')



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db.delta_tbl_partition
# MAGIC
