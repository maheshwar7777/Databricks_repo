# Databricks notebook source
# MAGIC %md
# MAGIC **Run the following cell to import the modules for running this notebook.** 

# COMMAND ----------

# MAGIC %run ./Includes/Imports

# COMMAND ----------

# MAGIC %md
# MAGIC # Full Load into Silver Tables
# MAGIC
# MAGIC In this step, we perform a **full load** from the **Bronze layer** into the **Silver layer**, applying data cleansing and standardization.

# COMMAND ----------

# MAGIC %md
# MAGIC **What it does:**
# MAGIC - Iterates over all table mappings defined in `bronze_to_silver_mapping`
# MAGIC - Reads each Bronze table using `readfrombronzeDF(...)`
# MAGIC - Drops duplicate records
# MAGIC - Applies table-specific transformations and data cleansing
# MAGIC - Writes the cleaned data to the corresponding **Silver Delta table**

# COMMAND ----------

# MAGIC %md
# MAGIC **Users can see the bronze_to_silver_mapping in the Includes/config.py**

# COMMAND ----------

print(bronze_to_silver_mapping)

# COMMAND ----------

def full_load_data_silver():
    for bronze_table, silver_table in bronze_to_silver_mapping.items():
        
        bronze_data_df = readfrombronzeDF(bronze_table)
        # Drop Duplicates
        exclude_drop_duplicate = ["ingestedfilepath","ingestiontime"]
        all_columns = bronze_data_df.columns
        dedup_columns = [c for c in all_columns if c not in exclude_drop_duplicate]
        bronze_data_df = bronze_data_df.dropDuplicates(subset=dedup_columns)
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
        bronze_data_df.write.format("delta").mode("append").saveAsTable(f"{silver_table}")
        print(f"Data written to {silver_table}")
        

# COMMAND ----------

# MAGIC %md
# MAGIC **Run the below cell to load the silver tables.**

# COMMAND ----------

full_load_data_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC **You can verify the full load of the silver tables by running the following cell.It will display the name of the table from silver schema.**

# COMMAND ----------

spark.sql(f'show tables from {catalog_name}.{silver_schema_name}').display()