# Databricks notebook source
# MAGIC %sql
# MAGIC """
# MAGIC Streamming data : Normally we create a streamming process when we need to run the job contiounly and we are expecting small amount data contiounlys.
# MAGIC
# MAGIC """

# COMMAND ----------

# reading streamming data 

# syntax :

df = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet').option('cloudFiles.schemaLocation','<Provide a location to stores the schemaCheckPoint file(like provide volumn path in catalog)>').load('<provide input file path>')

# we can also use below syntax to read the streaming data

df=spark.readStream.format('csv').option('header',True).schema(<schema_name>).option('sep',';').load('<place the input file path>')

display(df)

# COMMAND ----------

# to write the streamming data

# syntax:

df.writeStream.format("delta").trigger(availableNow=True).outputMode(
    "append"
).PartitionBy("Year", "Month", "Day").option(
    "checkpointLocation",
    "<provide checkpoint location to maintain the data till which it processed>",
).option(
    "mergeSchema", True
).queryName(
    "<table name with schema>"
).toTable(
    "<Table cache path>"
)


# we can also use below syntax to write the streaming data

df_1 = df.writeStream.format('parquet').outputMode('append').option('path','<place the path to write the data file>').option('checkpointLocation','<Place the path to store the checkpoin fies>').start()

or

df_1 = df.writeStream.format('parquet').outputMode('append').option('path','<place the path to write the data file>').option('checkpointLocation','<Place the path to store the checkpoin fies>').table('<palce table name>')

# COMMAND ----------

"""
There are three types of output mode. (Referring : outputMode("append"))

those are : 
1. append
2. complete
3. update

"""
