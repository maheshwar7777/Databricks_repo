# Databricks notebook source
import dlt
from pyspark.sql.functions import col, to_date, to_timestamp, lit, to_json, struct, current_timestamp
from config import *
from dq_silver_utility import *
from pyspark.sql.functions import *
import json
import uuid

# COMMAND ----------

config = json.loads(DQ_INPUT)
catalog = CATALOG
bronze_db = BRONZE_DATABASE
silver_db = SILVER_DATABASE



# COMMAND ----------

# -----------------------------------
# Function: Create Silver Table
# -----------------------------------
def create_generic_silver_table(table_name, table_config, catalog, bronze_db, silver_db):
    cleanup_config = table_config.get("cleanup_functions", {})
    validations = table_config.get("validations", {})

    if table_name in TABLE_LIST:
        rich_validations = {}
        for name, rule in validations.items():
            if isinstance(rule, str):
                rich_validations[name] = {"type": "expect_or_drop", "condition": rule}
            else:
                rich_validations[name] = rule

        for colname in cleanup_config.get("null_column_drop", {}).get("null_columns_check", []):
            rich_validations[f"{colname}_not_null"] = {
                "type": "expect_or_drop",
                "condition": f"{colname} IS NOT NULL"
            }

        expectations_decorator = apply_expectations(rich_validations)

        @dlt.table(
            name=f"{catalog}.{silver_db}.{table_name}",
            comment=f"Silver table for {table_name}"
        )
        @expectations_decorator
        def silver():
            df = dlt.read_stream(f"{catalog}.{bronze_db}.{table_name}")
            df = preprocess_derived_columns(df, cleanup_config)
            df = apply_cleanup_functions(df, cleanup_config)
            return df
    else:
        @dlt.table(
            name=f"{catalog}.{silver_db}.{table_name}",
            comment=f"Simple passthrough for {table_name}"
        )
        def passthrough():
            return dlt.read_stream(f"{catalog}.{bronze_db}.{table_name}")


# COMMAND ----------


# -----------------------------------
# Function: Create Quarantine Table
# -----------------------------------
def create_quarantine_table(config, catalog, bronze_db, silver_db):
    @dlt.table(name=f"{catalog}.{silver_db}.quarantine", comment="Quarantine records for failed validations")
    def quarantine():
        all_invalids = None

        for table_name, table_conf in config.get("tables", {}).items():
            validations = table_conf.get("validations", {})
            cleanup = table_conf.get("cleanup_functions", {})

            for rule_name, rule in list(validations.items()):
                if isinstance(rule, str):
                    validations[rule_name] = {"type": "expect_or_drop", "condition": rule}
            
            null_col_check = cleanup.get("null_column_drop", {}).get("null_columns_check", [])
            for colname in null_col_check:
                validations[f"{colname}_not_null"] = {
                    "type": "expect_or_drop",
                    "condition": f"{colname} IS NOT NULL"
                }

            df = dlt.read_stream(f"{catalog}.{bronze_db}.{table_name}")
            
            derived_cols = cleanup.get("derived_columns", {})
            for col_name, formula in derived_cols.items():
                df = df.withColumn(col_name, expr(formula))

            for rule_name, rule in validations.items():
                condition = rule.get("condition")
                if condition:
                    # invalids = df.filter(f"NOT ({condition})")
                    invalids = get_invalid_records(df, table_name, condition, rule_name)
                    all_invalids = invalids if all_invalids is None else all_invalids.unionByName(invalids, allowMissingColumns=True)

        return all_invalids

# COMMAND ----------

@dlt.table(name=f"{catalog}.{silver_db}.quarantine_summary_gold", comment="Summary of validation errors by table and reason")
def quarantine_summary_gold():
    quarantine_df = dlt.read_stream(f"{catalog}.{silver_db}.quarantine")
    return quarantine_df.groupBy("table_name", "reason") \
        .agg(
            sum("invalid_records").alias("invalid_record_count"),
            expr("max(logged_at)").alias("last_seen")
        )


# COMMAND ----------

# -----------------------------------
# Function: Quarantine Table
# -----------------------------------
def get_invalid_records(df, table_name, condition_expr, reason):
    return df.filter(f"NOT ({condition_expr})") \
        .withColumn("table_name", lit(table_name)) \
        .withColumn("reason", lit(reason)) \
        .withColumn("record_data", to_json(struct(*df.columns))) \
        .withColumn("invalid_records", lit(1).cast("long")) \
        .withColumn("logged_at", current_timestamp()) \
        .select("table_name", "reason", "record_data", "invalid_records", "logged_at")


# COMMAND ----------

# Generate Silver tables
for table_name in TABLE_LIST:
    table_config = config["tables"].get(table_name, {})  # if not defined, default to empty
    create_generic_silver_table(table_name, table_config, catalog, bronze_db, silver_db)


# Generate Quarantine Table
create_quarantine_table(config, catalog, bronze_db, silver_db)