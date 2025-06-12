# Databricks notebook source
# MAGIC %md
# MAGIC Views

# COMMAND ----------

df_1=spark.createDataFrame(data=[(1,'Mahesh'),(2,'Sam'),(3,'Reddy'),(4,('Jai'))],schema=['ID','Name'])

display(df_1)

# COMMAND ----------

# Types of view in pyspark.

# Below two view are same
    # Once golbal view it created, then it will be access in another notebook which connects to same cluster
    # df_1.createGlobalTempView  
    # df_1.createOrReplaceGlobalTempView

# Below two view are same
    # Once TEMP view it created, then it is alive in same notebook and it cannot used in another notebook.
    # df_1.createTempView
    # df_1.createOrReplaceTempView


# COMMAND ----------

# create a golbal temp view

df_1.createOrReplaceGlobalTempView('df_gbl_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- access global temp view (note always use shema - global_temp for global views)
# MAGIC
# MAGIC
# MAGIC select * from global_temp.df_gbl_view

# COMMAND ----------

# create a  temp view

df_1.createOrReplaceTempView('df_temp_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- access temp view
# MAGIC
# MAGIC select * from df_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --dropping temp view
# MAGIC
# MAGIC --drop  view df_temp_view

# COMMAND ----------

# converting table into dataframe

df_2=spark.read.table('df_temp_view')

display(df_2)

# approach 2 : on converting view data into dataframe:

df_3 = spark.sql('select * from df_temp_view')

df_3.display()

# COMMAND ----------



