# Databricks notebook source
# #  Create input paramters

# dbutils.widgets.text('fileName','')

# dbutils.widgets.text('File_format','')  -- it is used for text 

# dbutils.widgets.dropdown('File_format','csv',['csv','json','parquet'])



# COMMAND ----------

# Read file name into variables

File_name = dbutils.widgets.get('fileName')

F_format = dbutils.widgets.get('File_format')

print(File_name)

print(F_format)

# COMMAND ----------

# check the file format 

def checkFileFormat(F_format) :
    if F_format == 'csv':
        fileFormat = 'csv'
    elif F_format == 'json':
        fileFormat = 'csv'
    elif F_format == 'parquet':
        fileFormat = 'parquet'
    
    return fileFormat


# COMMAND ----------

# Create user defined functions

def loadInputFile (fileN, formt):
    try:
        df = spark.read.format(formt).option('header',True).option('inferSchema',True).load(fileN)

    except Exception as e :
        print(f" we have following issue  : {e}")
    
    else :
        return df


# COMMAND ----------

# display the dataframe

# file path : dbfs:/FileStore/tables/DBFiles/raw_data.csv
# file 2 path : /FileStore/tables/EmployeeAttrition/TestFile/WA_Fn_UseC__HR_Employee_Attrition.csv

format_file = checkFileFormat(F_format)

df = loadInputFile(File_name,format_file)

display(df)
