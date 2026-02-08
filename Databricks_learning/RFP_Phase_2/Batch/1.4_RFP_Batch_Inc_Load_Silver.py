# Databricks notebook source
# MAGIC %md
# MAGIC ## Incremental Load into Silver Tables
# MAGIC
# MAGIC This section handles **incremental data movement** from Bronze to Silver by reading only new records based on `ingestiontime`.
# MAGIC

# COMMAND ----------

# MAGIC %run ./Includes/Imports

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Incremental From Bronze
# MAGIC
# MAGIC - Reads the latest `ingestiontime` from the Silver table
# MAGIC - Filters the Bronze table for records with a **newer ingestion time**
# MAGIC - Returns only **incremental records** for further processing
# MAGIC

# COMMAND ----------

def readIncrementalFromBronze(bronze_table,silver_table):

    max_silver_table_timestamp=spark.sql(f"""select max(ingestiontime) latest_file_timestamp from {silver_table}""").collect()[0][0]

    bronze_ingestiontime_df=spark.sql(f"""select *,to_timestamp(concat(cast(load_date as string), cast(ingestiontime as string)), 'yyyyMMddHHmmss') as tr_ingestiontime from {bronze_table}""")

    bronze_ingestiontime_df=bronze_ingestiontime_df.drop("ingestiontime").withColumnRenamed("tr_ingestiontime","ingestiontime")

    incremental_df=bronze_ingestiontime_df.filter(f"""ingestiontime > '{max_silver_table_timestamp}'""")

    return incremental_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### merge_to_silver
# MAGIC
# MAGIC - Performs an **upsert (merge)** into the Silver Delta table using the provided `merge_keys`
# MAGIC - Updates existing rows and inserts new ones
# MAGIC - Uses Delta Lake’s `MERGE` command to handle duplicates and ensure atomicity

# COMMAND ----------

def merge_to_silver(bronze_df, bronze_table, silver_table, merge_keys):
    try:
        if bronze_df is None or bronze_df.isEmpty():
            print(f"[{bronze_table}]: No data to merge into {silver_table}")
            return

        silver_delta = DeltaTable.forName(spark, silver_table)
        condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])

        silver_delta.alias("target").merge(
            bronze_df.alias("source"),
            condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"Merged data into Silver table: {silver_table}")
    except Exception as e:
        print(f"Error merging data into Silver table: {silver_table}", e)

# COMMAND ----------

# MAGIC %md
# MAGIC **You can check the merge_key 'bronze_silver_merge_keys' in Includes/config.py. Run the below cell to view the merge key.**

# COMMAND ----------

print(bronze_silver_merge_keys)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental load data silver
# MAGIC
# MAGIC - Iterates over all Bronze to Silver table mappings
# MAGIC - Applies table-specific transformations (timestamp casting, null handling, field normalization, etc.)
# MAGIC - Deduplicates based on business keys 
# MAGIC - Merges the cleaned incremental data into the Silver table

# COMMAND ----------

# MAGIC %md
# MAGIC **Run the below cell to load the silver tables incrementally.**

# COMMAND ----------

def incremental_load_data_silver():
    for bronze_table, silver_table in bronze_to_silver_mapping.items():
        print(f"Processing table: {bronze_table}")
        keys = bronze_silver_merge_keys.get(bronze_table)

        if not keys:
            print(f"No merge keys found for table: {bronze_table}")
            continue

        bronze_data_df = readIncrementalFromBronze(bronze_table,silver_table)
        if bronze_data_df is not None:

        # Drop Duplicates
            exclude_drop_duplicate = ["ingestedfilepath","ingestiontime"]
            
            bronze_data_df = bronze_data_df.dropDuplicates(subset=bronze_silver_merge_keys[bronze_table])
            
        # Apply custom transformations based on the table
        if bronze_table.endswith("immunizations"):
            bronze_data_df = bronze_data_df.withColumn("immunization_date", col("immunization_date").cast("timestamp"))

        elif bronze_table.endswith("medications"):
            bronze_data_df = bronze_data_df.withColumn("stop_date",when(col("stop_date")=="None","9999-12-31T00:00:00Z").otherwise(col("stop_date")))
            bronze_data_df = bronze_data_df.withColumn("start_date", col("start_date").cast("timestamp")) \
                   .withColumn("stop_date", col("stop_date").cast("timestamp"))
            bronze_data_df  = bronze_data_df.fillna({
                       "stop_date": "9999-12-31T00:00:00Z",
                       "reasoncode": "NA",
                       "reasondescription": "NA",
                       })
        elif bronze_table.endswith("observations"):
            bronze_data_df = bronze_data_df.withColumn("observation_date", col("observation_date").cast("timestamp"))
            bronze_data_df=  bronze_data_df.fillna({
                       "category": "NA",
                       "units": "NA"
                        })
            
        elif bronze_table.endswith("procedures"):
            bronze_data_df = bronze_data_df.withColumn("stop_time",when(col("stop_time")=="None","9999-12-31T00:00:00Z").otherwise(col("stop_time")))
            bronze_data_df = bronze_data_df.withColumn("start_time", col("start_time").cast("timestamp")) \
                   .withColumn("stop_time", col("stop_time").cast("timestamp")) \
                    .fillna({
                       "reason_code": "None",
                       "reason_description": "None",
                     })
        elif bronze_table.endswith("providers"):
            bronze_data_df = bronze_data_df.withColumn("name", regexp_replace(col("name"), "[^a-zA-Z ]", "")) \
                    .withColumn('gender',when(col("gender")=="M","Male").when(col("gender")=="F","Female").otherwise("UNKNOWN")) \
                    .withColumn("encounters", col("encounters").cast("int")) \
                    .withColumn("procedures", col("procedures").cast("int")) \
                    .fillna({
                        "state": "NA",
                        "zip": "NA",
                        "lat": 0,
                        "lon": 0,
            })
        
        merge_to_silver(bronze_data_df, bronze_table, silver_table, keys)

incremental_load_data_silver()        

# COMMAND ----------

# MAGIC %md
# MAGIC **In the next step you can check the version history of the silver tables.You will be able to see the merge operation**

# COMMAND ----------

spark.sql(f'describe history {catalog_name}.{silver_schema_name}.providers').display()

# COMMAND ----------

# MAGIC %md
# MAGIC **You can check the data in the silver tables by running the following command.**

# COMMAND ----------

spark.sql(f'select * from {catalog_name}.{silver_schema_name}.providers').display()