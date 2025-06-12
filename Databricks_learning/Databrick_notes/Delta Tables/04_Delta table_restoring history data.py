# Databricks notebook source
# MAGIC %md
# MAGIC ## Using restore option, we can restore history data into the delta table.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db.delta_tbl_2

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history db.delta_tbl_2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --restoring version 2 data
# MAGIC
# MAGIC restore table db.delta_tbl_2 to version as of 3
# MAGIC
# MAGIC /* 
# MAGIC we can also use follwing syntax : select * from table_name version as of 3;
# MAGIC or
# MAGIC select * from delta.'file path' version as of 3;
# MAGIC */
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db.delta_tbl_2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --we can restore data using timestamp
# MAGIC
# MAGIC
# MAGIC restore table db.delta_tbl_2 to timestamp as of '2024-09-13T06:17:00.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db.delta_tbl_2
# MAGIC

# COMMAND ----------

# justing if we can captur hitstory records in dataframe

df=spark.createDataFrame(data=[(1,"Mahesh",30),(2,"Vishnu",40),(3,'Ali',50)],schema=['ID','Name','Age'])

df.write.format('delta').mode('overwrite').saveAsTable("tmp_dleta_tbl_00001")

spark.sql("""update tmp_dleta_tbl_00001 set age=60 where id=3 """)

df2=spark.sql("""desc history tmp_dleta_tbl_00001 """)

df2.display()

