# Databricks notebook source
display(dbutils.fs.ls('dbfs:/FileStore/tables/DBFiles/'))

# dbfs:/FileStore/tables/DBFiles/raw_data.csv --path for raw csv file
# dbfs:/FileStore/tables/DBFiles/raw_data.json --path for raw json file
# dbfs:/FileStore/tables/DBFiles/raw_data.xml  --path for raw xml file
# dbfs:/FileStore/tables/DBFiles/raw_data.parquet  --path for raw parquet file
# dbfs:/FileStore/tables/DBFiles/raw_data.xlsx --path for excel file

# syntax to create directries in DBFS

dbutils.fs.mkdirs('<place the path to create>')  # command to create the path in DBFS
dbutils.fs.rm('<path which needs to be removed >',True)   #commnd to remove the path in DBFS

# COMMAND ----------

df= spark.read.option('header',True).csv('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

display(df)
# df.count()  --print the count the rows in dataframe

# Another approch to read multiple file to dataframe (main thing is - we need pass the file name path in list)

# df_2 = spark.read.format('csv').option('header',True).load(['dbfs:/FileStore/tables/DBFiles/raw_data.csv','dbfs:/FileStore/tables/DBFiles/raw_data_2.csv'])

# display(df_2)




# COMMAND ----------

display(dbutils.data.summarize(df))

# COMMAND ----------

df= spark.read.option('header',True).csv('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

display(df)
df.printSchema()
df.schema

# COMMAND ----------

#loading multiline json file

df_json = spark.read.option('header',True).option('multiLine',True).json('dbfs:/FileStore/tables/DBFiles/raw_data.json')

display(df_json)

# COMMAND ----------

# loading incorrect csv raw file. creating schema for input file

from pyspark.sql.types import *

schema = StructType(
    [
        StructField("ID", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("DOB", DateType(), True),
        StructField("curpt_col", StringType(), True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC 3 type of reading mode into pyspark:
# MAGIC
# MAGIC 1.Permissive Mode : its a default mode. if spark is not able to parse it due to datatype mismatch then make it as null without impacting other result, i.e only issue record are null others are fine.
# MAGIC
# MAGIC 2.DROPMALFORMED : whatever record having parsing issue, i.e in any row there is issues it will totally ignore that record, it only show correct record.
# MAGIC
# MAGIC 3.FailFast Mode : while reading data into this mode it will fail as soon as it encounters any record have parsing issues.

# COMMAND ----------

# loading incorrect csv raw file

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Permissive mode
# df_incrt_csv = spark.read.option('header',True).schema(schema).option('mode','Permissive').csv('dbfs:/FileStore/tables/DBFiles/raw_data_2.csv')
# display(df_incrt_csv)

# DROPMALFORMED mode
# df_incrt_csv = spark.read.option('header',True).schema(schema).option('mode','DROPMALFORMED').csv('dbfs:/FileStore/tables/DBFiles/raw_data_2.csv')
# display(df_incrt_csv)

# #FailFast mode
# df_incrt_csv = spark.read.option('header',True).schema(schema).option('mode','FailFast').csv('dbfs:/FileStore/tables/DBFiles/raw_data_2.csv')
# display(df_incrt_csv)


df_incrt_csv = (
    spark.read.option("header", True)
    .schema(schema)
    .option("columnNameOfCorruptRecord", "curpt_col")
    .csv("dbfs:/FileStore/tables/DBFiles/raw_data_2.csv")
)
df_incrt_csv_1 = df_incrt_csv.filter(col('curpt_col').isNotNull())  #.select('*')

df_incrt_csv_1.select(col('*')).show(truncate=False)

# display(df_incrt_csv_1)




# COMMAND ----------

# loading  raw xml file

from pyspark.sql.functions import *
from pyspark.sql.types import *

df_xml = (
    spark.read.option("header", True)
    .option("rowTag", "row")
    .xml("dbfs:/FileStore/tables/DBFiles/raw_data.xml")
)

display(df_xml)

# COMMAND ----------

# Loading parquet file

from pyspark.sql.functions import *
from pyspark.sql.types import *

df_par = (
    spark.read.option("header", True)
    .parquet("dbfs:/FileStore/tables/DBFiles/raw_data.parquet")
)

display(df_par)

# COMMAND ----------

# MAGIC %md
# MAGIC ##--------------- Loading Excel file -------------------
# MAGIC
# MAGIC Install below one in cluster :
# MAGIC
# MAGIC library--> install now --> Maveen --> Coordinate : com.crealytics:spark-excel_2.12:0.13.5
# MAGIC

# COMMAND ----------

# loading excel file

from pyspark.sql.functions import *
from pyspark.sql.types import *

df_excel = (
    spark.read.format('com.crealytics.spark.excel').option("header", True)
    .load("dbfs:/FileStore/tables/DBFiles/raw_data.xlsx")
)

display(df_excel)
