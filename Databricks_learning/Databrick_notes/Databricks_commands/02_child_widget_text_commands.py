# Databricks notebook source
# below commands to create text widgets

dbutils.widgets.text("Folder_name", "","")
dbutils.widgets.text("File_name", "","")

# COMMAND ----------

# to the widgets input to local variables

folder_location = dbutils.widgets.get("Folder_name")
file_location = dbutils.widgets.get("File_name")

print(f'folder location is : {folder_location}')
print(f'file location is : {file_location}')

# COMMAND ----------

# get the folder and file location and read the file and store it in dataframe

df= spark.read.format('csv').option('header',True).option('inferSchema',True).load(folder_location+file_location)

display(df)
