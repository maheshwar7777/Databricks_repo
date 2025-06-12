# Databricks notebook source
display(dbutils.fs.ls('dbfs:/FileStore/tables/DBFiles/raw_data.csv'))

df=spark.read.format('csv').option('header',True).option('inferSchema',True).load('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

display(df)

# dbfs:/FileStore/tables/DBFiles/raw_data.csv --path for raw csv file

# COMMAND ----------

# import pandas library

import pandas as pd

# COMMAND ----------

# Read CSV file into dataframe

# df= pd.read_csv('/dbfs:/FileStore/tables/DBFiles/raw_data.csv')

data = {'col1' : [1,23,3,4] , 'col2' : [2,3,3,44]}

df_1 = pd.DataFrame(data)

df_1

# COMMAND ----------

import pandas as pd
# import koalas as pd
# from dbutils import read_csv

# Read CSV file into dataframe

df= pd.read_csv("/FileStore/tables/FlipkartProject/testFile/Flipkart.csv")

