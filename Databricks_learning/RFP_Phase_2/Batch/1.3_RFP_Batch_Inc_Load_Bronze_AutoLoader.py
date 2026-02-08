# Databricks notebook source
# MAGIC %md
# MAGIC ## Incremental Load to Bronze using Auto Loader
# MAGIC
# MAGIC This section sets up **incremental ingestion pipelines** from raw JSON files into **Bronze Delta tables** using **Databricks Auto Loader** .
# MAGIC
# MAGIC **Auto Loader** detects and processes new files as they arrive in cloud storage (e.g., ADLS, S3, GCS)
# MAGIC - Supports **schema inference and evolution**
# MAGIC - Tracks ingestion state using **checkpointing** (no manual tracking required)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ./Includes/Imports

# COMMAND ----------

# MAGIC %md
# MAGIC **Run this step to load new json files.**

# COMMAND ----------

for table_name in bronze_tables_list:
    load_new_files_bronze(table_name,num_rows,output_base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC **You can check the new files in Volumes/table/date_folder.**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **What it does:**
# MAGIC - Monitors a directory for new JSON files using Auto Loader and applies the schema
# MAGIC - Applies the defined schema for each table 
# MAGIC - Extracts metadata like:
# MAGIC   - ingestedfilepath: the full file path
# MAGIC   - load_date: date from the file path
# MAGIC   - ingestiontime: time from the file name
# MAGIC - Ingests only new data using Structured Streaming with availableNow = True
# MAGIC - Appends the parsed data to the corresponding **Bronze Delta table**
# MAGIC - Writes data to the corresponding Bronze Delta table using **Structured Streaming**
# MAGIC

# COMMAND ----------

def get_incremental_data(table_name,output_base_path):
    inc_df=spark.readStream\
        .format("cloudFiles")\
            .option("cloudFiles.format", "json")\
                .option('multiline','True')\
                    .schema(table_schema_map[table_name])\
                        .load(f"{output_base_path}/{table_name}/")\
                            .withColumn("ingestedfilepath", col("_metadata.file_path"))\
                                .withColumn('load_date',substring(col('ingestedfilepath'),-19,8))\
                                    .withColumn('ingestiontime',substring(col('ingestedfilepath'),-11,6))
   

    inc_df.writeStream\
        .format("delta")\
            .option("checkpointLocation", f"{output_base_path}/checkpoint/{table_name}/")\
                .outputMode("append").\
                    trigger(availableNow=True)\
                        .table(f"{catalog_name}.{bronze_schema_name}.{table_name}")
    print(f"Table {table_name} is incrementally loaded")

for table_name in bronze_tables_list:
    get_incremental_data(table_name,output_base_path)
    

# COMMAND ----------

# MAGIC %md
# MAGIC **In the next step you can check the version history of the bronze table.You will be able to see the streaming update operation**

# COMMAND ----------

spark.sql(f'describe history {catalog_name}.{bronze_schema_name}.providers').display()

# COMMAND ----------

# MAGIC %md
# MAGIC **You can check the data in the bronze tables by running the following command.**

# COMMAND ----------

spark.sql(f'select * from {catalog_name}.{bronze_schema_name}.providers').display()