# Databricks notebook source
"""
Vacuum : Using this option, we can delete older files/ version file which are not helpful to us. Since older file take more file and cost to keep them

"""

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc extended db.delta_tbl_2

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/db.db/delta_tbl_2'))

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history db.delta_tbl_2

# COMMAND ----------

# turnning of spark config setting to vacuum file for 1 hour. idle time is 1 week

spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', False)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --now vacuum the file for one hr
# MAGIC vacuum db.delta_tbl_2 retain 1 hours 

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/db.db/delta_tbl_2'))

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history db.delta_tbl_2

# COMMAND ----------

df_1=spark.range(1,5)

display(df_1)
