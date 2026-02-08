# Databricks notebook source
# MAGIC %md
# MAGIC ### **Please run the cells one by one.Running all will lead to run the cleanup cell as well.**

# COMMAND ----------

# MAGIC %run ./Includes/Imports

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh Gold Tables from Silver
# MAGIC
# MAGIC This step updates the **Gold-level summary tables** by re-aggregating data from the **Silver tables**.

# COMMAND ----------

# MAGIC %md
# MAGIC **What it does:**
# MAGIC - Iterates over each Silver to Gold 
# MAGIC - Reads and deduplicates Silver data
# MAGIC - Applies business-specific aggregation logic
# MAGIC - **Overwrites** the corresponding Gold table with fresh metrics

# COMMAND ----------

def inc_load_data_gold():

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

inc_load_data_gold()

# COMMAND ----------

# MAGIC %md
# MAGIC **In the next step you can check the version history of the Gold tables.**

# COMMAND ----------

spark.sql(f'describe history {catalog_name}.{gold_schema_name}.medication_cost_summary').display()

# COMMAND ----------

# MAGIC %md
# MAGIC **You can check the data in the gold tables by running the following command.**

# COMMAND ----------

spark.sql(f'select * from {catalog_name}.{gold_schema_name}.medication_cost_summary').display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # CLEANUP
# MAGIC **Warning: Please run the following command only if you are done with all the operations on the catalog.It will clean up the catalog.**

# COMMAND ----------

spark.sql(f"drop catalog if exists `{catalog_name}` cascade")