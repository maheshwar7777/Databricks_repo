# Databricks notebook source
"""
mergeSchema : Suppose we create a delta table with dataframe scehma and later(next day), data frame schema got changed (means new column added). in that case, we can use mergeWrite to have latest schema (having both intial and latest data).
            : MergeSchema is used in incremental data

overwriteSchema : in overwriteSchema , we get latest scehma and latest data into delta table. We cannot use append mode, so to use append mode, we need to use mergeSchema option along with overwriteSchema.
                : overwriteSchema is used in full load data

"""

# COMMAND ----------

# create a dataframe

data = [(1,'Mahesh',3000,2000),(2,'Ramesh',4000,100),(3,'Suresh',2000,1000),(4,'Ram',30000,2000)]
schema = ['ID','Name','Salary','Bonus']

df=spark.createDataFrame(data,schema)

display(df)

# COMMAND ----------

# Write dataframe data to delta table

df.write.mode('overwrite').saveAsTable('db.delta_temp_3')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from db.delta_temp_3
# MAGIC

# COMMAND ----------

# create a dataframe with new updated schema

data = [(1,'Mahesh',3000,2000,34),(2,'Ramesh',4000,100,32),(3,'Suresh',2000,1000,43),(4,'Ram',30000,2000,65)]
schema = ['ID','Name','Salary','Bonus','Age']

df=spark.createDataFrame(data,schema)

display(df)

# COMMAND ----------

# write the updated schema data frame to same delta table

df.write.mode('append').option("mergeSchema", "true").saveAsTable('db.delta_temp_3')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db.delta_temp_3

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table db.delta_temp_3

# COMMAND ----------

# MAGIC %md
# MAGIC # OverWriteSchema `method`
# MAGIC

# COMMAND ----------

# create a dataframe

data = [(1,'Mahesh',3000,2000),(2,'Ramesh',4000,100),(3,'Suresh',2000,1000),(4,'Ram',30000,2000)]
schema = ['ID','Name','Salary','Bonus']

df=spark.createDataFrame(data,schema)

display(df)

# COMMAND ----------

# Write dataframe data to delta table

df.write.mode('overwrite').saveAsTable('db.delta_temp_4')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db.delta_temp_4

# COMMAND ----------

# create a dataframe with new updated schema

data = [(1,'Mahesh',3000,2000,34),(2,'Ramesh',4000,100,32),(3,'Suresh',2000,1000,43),(4,'Ram',30000,2000,65)]
schema = ['ID','Name','Salary','Bonus','Age']

df=spark.createDataFrame(data,schema)

display(df)

# COMMAND ----------

# write the updated schema data frame to same delta table

df.write.mode('append').option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable('db.delta_temp_4')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db.delta_temp_4
# MAGIC
