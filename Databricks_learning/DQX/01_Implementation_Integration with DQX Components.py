# Databricks notebook source
# MAGIC %md
# MAGIC The following code demonstrates a full data quality workflow using DQX:

# COMMAND ----------

# MAGIC %md
# MAGIC Installing the DQX Library
# MAGIC

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

# MAGIC %md
# MAGIC Restart Python Kernel

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Importing required libraries

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import explode, col, lit
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx import check_funcs
import yaml
import logging

# COMMAND ----------

# MAGIC %md
# MAGIC Setting up logging

# COMMAND ----------

logging.basicConfig(level=logging.INFO)

# COMMAND ----------

# MAGIC %md
# MAGIC Initializing the DQ Engine

# COMMAND ----------

dq_engine = DQEngine(WorkspaceClient())

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a DataFrame with data quality issues

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

df = spark.createDataFrame(
    data=[
        (1, "Alice", "alice@example.com", 30, "2022-01-15", "Female"),
        (2, "Bob", "bob@example.com", 25, "2022-14-01", "Male"),
        (3, "Charlie", "charlie@example.com", None, "2022-03-01", "Femail"),
        (4, "Joanna", "joice@example.com", 30, "2022-01-15", "Female"),
        (5, "Eve", None, 200, "2022-02-30", "Female"),
        (None, "Frank", "frank@example.com", 28, "2022-05-20", "F"),
    ],
    schema=StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("signup_date", StringType(), True),
        StructField("gender", StringType(), True),
    ])
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC DQProfiler - Profile the data

# COMMAND ----------

profiler = DQProfiler(WorkspaceClient())
summary_stats, profiles = profiler.profile(df, options={"fraction": 1.0})

print("Print summary_stats")
print(yaml.safe_dump(summary_stats))

print("Profiles:")
for profile in profiles:
    print(profile)

# COMMAND ----------

# MAGIC %md
# MAGIC DQGenerator - Generate DQ Rules

# COMMAND ----------

generator = DQGenerator(WorkspaceClient())
generated_checks = generator.generate_dq_rules(profiles)
print("Generated Checks (YAML):")
print(yaml.safe_dump(generated_checks))

# COMMAND ----------

# MAGIC %md
# MAGIC Defining DQ Rules manually here im manually defining YAML checks for the main DQEngine run.

# COMMAND ----------

yaml_checks = yaml.safe_load("""
- check:
    function: is_not_null
    arguments:
      column: id
    criticality: error
    name: id_is_null
- check:
    function: is_in_range
    arguments:
      column: age
      min_limit: 10
      max_limit: 100
    criticality: error
    name: age_isnt_in_range
- check:
    function: is_valid_date
    arguments:
      column: signup_date
    criticality: error
    name: wrong_date_format
- check:
    function: is_in_list
    arguments:
      allowed:
        - Female
        - Male
      column: gender
    criticality: error
    name: gender_is_not_in_the_list
- check:
    function: is_not_null
    arguments:
      column: email
    criticality: error
    name: email_is_null
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Validating the checks before execution

# COMMAND ----------

validated_checks = dq_engine.validate_checks(yaml_checks)
print("Checks validated successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC DQEngine - Apply checks and split the DataFrame with Valid DataFrame and Quarantined DataFrame

# COMMAND ----------

# Execute checks
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(df, yaml_checks)
display(quarantined_df)
display(valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Defining Run Checks Using Python

# COMMAND ----------

rule_checks = [
    DQRowRule(
        name="id_is_null",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="id",
    ),
    DQRowRule(
        name="age_isnt_in_range",
        criticality="error",
        check_func=check_funcs.is_in_range,
        column="age",
        check_func_kwargs={"min_limit": 10, "max_limit": 100},
    ),
    DQRowRule(
        name="wrong_date_format",
        criticality="error",
        check_func=check_funcs.is_valid_date,
        column="signup_date"
    ),
    DQRowRule(
        criticality="error",
        check_func=check_funcs.is_in_list,
        column="gender",
        check_func_kwargs={"allowed": ["Female", "Male"]},
    )
]

# Execute checks
valid_df, quarantined_df = dq_engine.apply_checks_and_split(df, rule_checks)
display(quarantined_df)
display(valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Applying quality rules and flag invalid records as additional columns (`_warning` and `_error`)

# COMMAND ----------

valid_and_quarantined_df = dq_engine.apply_checks_by_metadata(df, yaml_checks)  # for yaml defined checks
valid_and_quarantined_df = dq_engine.apply_checks(df, rule_checks)  # for python defined checks

# Methods to get valid and invalid dataframes
display(dq_engine.get_valid(valid_and_quarantined_df))
display(dq_engine.get_invalid(valid_and_quarantined_df))

display(valid_and_quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Generate a Validation Summary Log to count failures

# COMMAND ----------

def get_validation_summary_log(dqx_df_errors: DataFrame):
    rows = (
        dqx_df_errors
        .select(explode("_errors").alias("err"))
        .groupby(col("err.name").alias("failed_check"))
        .count()
        .collect()
    )
    lines = [
        f'Found {r["count"]} records with failed check "{r["failed_check"]}"'
        for r in rows
    ]
    validation_summary_log = "\n".join(lines)
    return validation_summary_log

print(get_validation_summary_log(quarantined_df))