# Databricks notebook source
# MAGIC %md
# MAGIC # Full Load into Gold Tables
# MAGIC
# MAGIC This notebook performs a **full load** from curated **Silver tables** into **Gold-level aggregated fact tables**.
# MAGIC

# COMMAND ----------

# MAGIC %run ./Includes/Imports

# COMMAND ----------

# MAGIC %md
# MAGIC **What it does:**
# MAGIC #Add the config information for the mapping
# MAGIC - Iterates through the `silver_to_gold_mapping` configuration
# MAGIC - Reads data from each Silver table
# MAGIC - Drops duplicate records 
# MAGIC - Applies business-specific aggregations and summarizations
# MAGIC - Writes the final output to the corresponding Gold Delta table (using `overwrite` mode)

# COMMAND ----------

# MAGIC %md
# MAGIC **Users can see the silver_to_gold_mapping in the Includes/config.py**

# COMMAND ----------

print(silver_to_gold_mapping)

# COMMAND ----------

def full_load_data_gold():
    for mapping in silver_to_gold_mapping:
        silver_table = mapping["silver_table"]
        gold_table = mapping["gold_table"]
        silver_data_df = readfromsilverDF(silver_table)
         # Drop Duplicates
        exclude_drop_duplicate = ["ingestiontime"]
        all_columns = silver_data_df.columns
        dedup_columns = [c for c in all_columns if c not in exclude_drop_duplicate]
        silver_data_df = silver_data_df.dropDuplicates(subset=dedup_columns)
        # Apply custom transformations based on the table
        if silver_table.endswith("immunizations"):
            silver_data_df = silver_data_df.groupBy("code", "description") \
                                           .agg(
                                               max("immunization_date").alias("last_administered"),
                                               count("*").alias("total_immunizations"),
                                               avg("base_cost").alias("avg_cost")
                                           )

        elif silver_table.endswith("observations"):
            silver_data_df = silver_data_df.groupBy("code", "description", "category")\
                                           .agg(
                                               max("observation_date").alias("last_observation"),
                                               count("*").alias("observation_count")
                                           )

        elif silver_table.endswith("medications"):
            if gold_table.endswith("medication_cost_summary_of_patients"):
                silver_data_df = silver_data_df.groupBy("code", "description", "patient", "encounter") \
                                           .agg(
                                               count("*").alias("total_prescriptions"),
                                               avg("base_cost").alias("avg_base_cost"),
                                               avg("payer_coverage").alias("avg_payer_coverage"),
                                               sum("totalcost").alias("total_spend")
                                           )
            elif gold_table.endswith("medication_cost_summary"):
                silver_data_df = silver_data_df.groupBy("code", "description") \
                                           .agg(
                                               count("*").alias("total_prescriptions"),
                                               avg("base_cost").alias("avg_base_cost"),
                                               avg("payer_coverage").alias("avg_payer_coverage"),
                                               sum("totalcost").alias("total_spend")
                                           )
                                          
        
        try:
            silver_data_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)
            print(f"Successfully written to gold table: {gold_table}")
        except Exception as e:
            print(f"Error writing to table {gold_table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Run the below cell to load the gold layer tables.**

# COMMAND ----------

full_load_data_gold()

# COMMAND ----------

# MAGIC %md
# MAGIC **You can verify the full load of the gold tables by running the following cell.It will display the name of the table from gold schema.**

# COMMAND ----------

spark.sql(f'show tables from {catalog_name}.{gold_schema_name}').display()