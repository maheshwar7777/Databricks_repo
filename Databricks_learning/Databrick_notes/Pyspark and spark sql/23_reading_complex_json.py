# Databricks notebook source
# Creating nested json file in dbfs path
# syntax :
# dbutils.fs.put("<place the file path where we need to create>","""[<place json object]""",True)  
        # Where Ture : means override if file already available

dbutils.fs.put("dbfs:/input/complex_json/01_nested_json_file.json","""[{
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}]""",True)

display(dbutils.fs.ls('dbfs:/input/complex_json'))


# COMMAND ----------

# Read the nested json file

df_json=spark.read.format("json").option("multiLine",True).load("dbfs:/input/complex_json/01_nested_json_file.json")

display(df_json)

# COMMAND ----------

# Check the dataframe schema

df_json.printSchema()

# COMMAND ----------

# un-construte the nested json step-by-step
"""
Complex data type:
    1) Struct - Dict
    2) Array  - List
    3) Map - 

How to flatern complex datatype:
    1) We can use explode() function for array datatype



"""



# COMMAND ----------

# un-construte the nested json step-by-step
from pyspark.sql.functions import *

df_json_step1 = df_json.select(col('id'),col('type'),col('name'),col('ppu'),explode(col("batters.batter")).alias('batter_upd'),col('topping')  )

display(df_json_step1)

# COMMAND ----------

df_json_step_2 = df_json_step1.select(col('*'),explode(col('topping')).alias('topping_upd'))

df_json_step_2.display()

# COMMAND ----------

df_json_step_fnl = df_json_step_2.select (col('*'), col('batter_upd.id').alias('batter_id'),col('batter_upd.type').alias('batter_type'),
                       col('topping_upd.id').alias('topping_Id') , col('topping_upd.type').alias('topping_type')                     
                        ).drop('batter_upd','topping_upd','topping')

# df_json_step_fnl.display()
display(df_json_step_fnl)
