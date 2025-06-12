# Databricks notebook source
# Create data frame and handle null values

data = [(1,'Rram',34),(2,'Sam',None),(3,' ',76),(None,'Bhim',43),(5,None,None)]

schema = ['ID','Name','Age']
df = spark.createDataFrame(data,schema
                            )

df.show()

# COMMAND ----------

# use na.fill() method --- (pass 0 to replace the integer column and pass '0' to replace the string columns)
# we can use fillna() method

df.na.fill('0').show()

# COMMAND ----------

# use below approach to handle null values only for specified column 

df.na.fill(0,['ID']).show()

# COMMAND ----------

# use below approach to handle null values mutli columns

df.na.fill({"ID":0,'Name':'0','Age':0}).show()

# COMMAND ----------

#  Dropna : To drop the null values using the dropna method
# syntax df.dropna(how=”any”, thresh=None, subset=None)
#  There are 3 parameter in dropna function.
    # 1-> Any
    # 2-> All
    # 3-> Subser = [<column name>]

    # Note : By default any is applied

# if any row having any Null then we are dropping that rows

df.dropna(how="any").display()


# COMMAND ----------

# if any row having all Null then we are dropping that rows

df.dropna(how="all").display()


# COMMAND ----------

# We have passed the thresh=2 parameter in the dropna() function which means that if there are any rows or columns which is having fewer non-NULL values than thresh values then we are dropping that row or column from the Dataframe. 

# if thresh value is not satisfied then dropping that row
df.dropna(thresh=2).display()


# COMMAND ----------

# we have passed the subset='City' parameter in the dropna() function which is the column name in respective of City column if any of the NULL value present in that column then we are dropping that row from the Dataframe. 

# if the subset column any value is NULL then dropping that row

df.dropna(subset="Age").display()

# COMMAND ----------

# we have passed (thresh=2, subset=(“Id”,”Name”,”City”)) parameter in the dropna() function, so the NULL values will drop when the thresh=2 and subset=(“Id”,”Name”,”City”) these both conditions will be satisfied means among these three columns dropna function checks whether thresh=2 is also satisfying or not, if satisfied then drop that particular row or column. 

# if thresh value is satisfied with subset column then dropping that row

df.dropna(thresh=2,subset=("Id","Name")).display()

