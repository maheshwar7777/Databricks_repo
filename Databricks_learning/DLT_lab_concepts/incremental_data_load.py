# Databricks notebook source
# MAGIC %md
# MAGIC #Incremental data ingention to bronze tables

# COMMAND ----------

from pyspark.sql import DataFrame


# COMMAND ----------


dbutils.widgets.text("entity_name", "")

# COMMAND ----------


entity=dbutils.widgets.get("entity_name")

print(entity)

# COMMAND ----------


def data_ingestion_fun(entity:str):
    df= spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.schemaLocation", f"/Volumes/dlt_catalog/dlt/dlt_volume/bronze/{entity}/checkpointing") \
        .option("cloudFiles.schemaEvolutionMode", "rescue") \
        .load(f"/Volumes/dlt_catalog/dlt/dlt_volume/input_files/{entity}/*.csv") 

    print("Input file read succesfully")
    return df




# COMMAND ----------

def write_to_table(df: DataFrame, entity: str):
    df.writeStream.outputMode("append") \
        .option("checkpointLocation", f"/Volumes/dlt_catalog/dlt/dlt_volume/bronze/{entity}/checkpointing") \
        .trigger(availableNow=True) \
        .table(f"{C_CATALOG}.{DLT_DATABASE}.bronze_{entity}")

    print(f"Data written to table bronze_{entity}")


# COMMAND ----------

# read input file and create bronze table

Input_df=data_ingestion_fun(entity)
# write to tagrt table

write_to_table(Input_df, entity)


