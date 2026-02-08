# Databricks notebook source
# MAGIC %md
# MAGIC # Getting Started with RFP Batch Load
# MAGIC ## Bronze Layer
# MAGIC This notebook helps simulate **Bronze-layer ingestion**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. It will also set your catalog,schema and volume. All tables will be read from and written to this location.
# MAGIC

# COMMAND ----------

# MAGIC %run ./Includes/Env_setup

# COMMAND ----------

# MAGIC %md
# MAGIC **Run the following cell to print the catalog name,bronze schema,silver schema and gold schema.**

# COMMAND ----------

print(f"catalog name: {catalog_name}")
print(f"bronze schema name: {bronze_schema_name}")
print(f"silver schema name: {silver_schema_name}")
print(f"gold schema name: {gold_schema_name}")
print(f"output base path: {output_base_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC In this section, you will:
# MAGIC - Generate records for a given healthcare domain (e.g., **immunizations**, **observations**, etc.)
# MAGIC - Write those records as **JSON files** in a date-based partitioned folder structure
# MAGIC - Use this data for testing Auto Loader / Bronze ingestion pipelines
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `table_name`: Name of the table to generate data for (e.g., `immunizations`, `medications`)
# MAGIC - `batch_count`: Number of records to generate (⚠️ must be ≤ 100000)
# MAGIC - `output_base_path`: The base directory where the data will be saved

# COMMAND ----------

def load_files_bronze(table_name,batch_count,output_base_path):
    
    assert batch_count <= 100000, "please don't go above 100000 writes, the generator will run for a too long time"
    
    data_list=tables_list_map[table_name]

    for i in range(batch_count):
        if table_name=="procedures":
            data_list.append(generate_procedures())
        elif table_name=="providers":
            data_list.append(generate_providers())
        elif table_name=="observations":
            data_list.append(generate_observations())
        elif table_name=="medications":
            data_list.append(generate_medications())
        elif table_name=="immunizations":
            data_list.append(generate_immunizations())

    folder_date=f"""{datetime.now().year}{datetime.now().month:02d}{datetime.now().day:02d}"""

    file_timestamp=f"""{datetime.now().year}{datetime.now().month:02d}{datetime.now().day:02d}{datetime.now().hour:02d}{datetime.now().minute:02d}{datetime.now().second:02d}"""

    os.makedirs(f"{output_base_path}/{table_name}/{folder_date}/", exist_ok=True)

    write_json_files(f"{output_base_path}/{table_name}/{folder_date}/{table_name}_{file_timestamp}.json", data_list)

# COMMAND ----------

# MAGIC %md
# MAGIC This step loads the previously generated raw JSON files into the **Bronze layer** of the Lakehouse.
# MAGIC

# COMMAND ----------

def full_load_table_bronze(table_name,output_base_path):
    data_df=spark.read.schema(table_schema_map[table_name])\
        .option('multiline','True')\
            .json(f"{output_base_path}/{table_name}/**/*.json")
    
    data_df=data_df\
    .withColumn("ingestedfilepath", col("_metadata.file_path"))\
        .withColumn('load_date',substring(col('ingestedfilepath'),-19,8))\
            .withColumn('ingestiontime',substring(col('ingestedfilepath'),-11,6))

    data_df.write.format("delta") \
    .mode("append") \
        .saveAsTable(f"{catalog_name}.{bronze_schema_name}.{table_name}")
    

# COMMAND ----------

# MAGIC %md
# MAGIC **Run the below cell to see bronze_tables_list. User can navigate to Includes/config.py to go through the variables.**

# COMMAND ----------

print(bronze_tables_list)

# COMMAND ----------

# MAGIC %md
# MAGIC **Run the below cell to load the bronze tables.**

# COMMAND ----------


for table_name in bronze_tables_list:
    load_files_bronze(table_name,num_rows,output_base_path)
    full_load_table_bronze(table_name,output_base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC **You can verify the full load of the bronze tables by running the following cell.It will display the name of the table from bronze schema.**

# COMMAND ----------

spark.sql(f'show tables from {catalog_name}.{bronze_schema_name}').display()