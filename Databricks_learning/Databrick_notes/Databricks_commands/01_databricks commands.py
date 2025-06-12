# Databricks notebook source
# MAGIC %md
# MAGIC # dbutils fs help 

# COMMAND ----------

dbutils.fs.help()

#we get help for file system methods like :

dbutils.fs.help('cp')   # here we are check the copy method

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # dbutils notebook help

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md
# MAGIC # dbuilts widget help

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # dbutils secrets help

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# commad to run  another notebook.
# path : /Users/kasireddymahesh7777@gmail.com/Databrick_notes/Pyspark and spark sql/00_File_load_to_dataframes

dbutils.notebook.run ("/00_File_load_to_dataframes",60)

#here first parameter --> notbook name and 2nd paramter--> timeout seconds

# note : we need  databricks premium subscription to run above command

# COMMAND ----------

# DBTITLE 1,# another approcah to call and run the another notebooks and pass the parameters
# MAGIC %run  ./file_name  $Folder_name = "dbfs:/FileStore/tables/DBFiles/" $File_name = "raw_data.csv"
# MAGIC

# COMMAND ----------

# differen types of widgets 

# dbutils.widgets.dropdown("Drop_down", "1",  [str(x) for x in range(1,10)])

# dbutils.widgets.combobox("Combo_box", "1",  [str(x) for x in range(1,10)])

# dbutils.widgets.multiselect("product", "netflix",  ("netflix", "zee", "maatv"))   

# COMMAND ----------

# to remove any widgets from notebook, we can use below command

dbutils.widgets.remove("Drop_down")

# COMMAND ----------

# to remove all widgets from notebook, we can use below command

dbutils.widgets.removeAll()
