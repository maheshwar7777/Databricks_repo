import yaml
import dlt
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, lower, expr, current_date, lit, row_number
from pyspark.sql.functions import *
import dlt
from pyspark.sql.functions import current_timestamp, lit, to_json, struct


def load_config(config_path: str):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_invalid_records(df: DataFrame, table_name: str, rule, clause: str):
    return df.filter(f"NOT ({clause})") \
        .withColumn("dq_table", lit(table_name).cast("string")) \
        .withColumn("dq_failed_column", lit(rule["column"]).cast("string")) \
        .withColumn("dq_failed_rule", lit(rule["rule"]).cast("string")) \
        .withColumn("dq_reason", lit(str(clause)).cast("string")) \
        .withColumn("record_data", to_json(struct(*df.columns)).cast("string")) \
        .withColumn("logged_at", current_timestamp()) \
        .select("dq_table", "dq_failed_column", "dq_failed_rule", "record_data", "dq_reason", "logged_at")



def cleanse_columns(df, cleansing_rules):
    for rule in cleansing_rules:
        col_name = rule["column"]
        if rule["operation"] == "trim":
            df = df.withColumn(col_name, trim(col(col_name)))
        elif rule["operation"] == "lower":
            df = df.withColumn(col_name, lower(col(col_name)))
    return df


def add_derived_columns(df, derived_columns):
    for col_def in derived_columns:
        df = df.withColumn(col_def["name"], expr(col_def["expression"]))
    return df


def drop_duplicates(df, keys, order_by):
    if not keys:
        return df
    if order_by:
        ordering = [col(c.strip().split()[0]).desc() if "DESC" in c else col(c.strip().split()[0]) for c in order_by]
        window_spec = Window.partitionBy(*keys).orderBy(*ordering)
        return df.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")
    else:
        return df.dropDuplicates(keys)


def build_expectation_clause(rule):
    col_name = rule["column"]
    if rule["rule"] == "not_null":
        return f"{col_name} IS NOT NULL"
    elif rule["rule"] == "range":
        return f"{col_name} BETWEEN {rule.get('min', 0)} AND {rule.get('max', 100)}"
    elif rule["rule"] == "in_list":
        allowed = ', '.join([f"'{v}'" for v in rule["allowed_values"]])
        return f"{col_name} IN ({allowed})"
    elif rule["rule"] == "regex":
        return f"{col_name} RLIKE '{rule['pattern']}'"
    elif rule["rule"] == "date_past":
        return f"{col_name} < current_date()"
    elif rule["rule"] == "date_future":
        return f"{col_name} > current_date()"
    else:
        return "TRUE"

def collect_invalids(invalid_dfs):
    all_invalids = None
    for invalid_df in invalid_dfs:
        all_invalids = invalid_df if all_invalids is None else all_invalids.unionByName(invalid_df, allowMissingColumns=True)
    return all_invalids


def apply_yaml_expectations(table_name: str, config_path: str):
    config = load_config(config_path)
    table_conf = config["tables"].get(table_name, {})
    expectations = table_conf.get("expectations", [])

    # Convert expectations list to the dictionary structure you designed
    expectation_config = {
        f"{rule['column']}_{rule['rule']}": {
            "type": rule.get("type", "expect"),  # Default to 'expect'
            "condition": build_expectation_clause(rule)
        }
        for rule in expectations
    }

    def decorator(f):
        for name, rule in reversed(expectation_config.items()):
            rule_type = rule.get("type", "expect")
            condition = rule.get("condition")

            if rule_type == "expect":
                f = dlt.expect(name, condition)(f)
            elif rule_type == "expect_or_drop":
                f = dlt.expect_or_drop(name, condition)(f)
            elif rule_type == "expect_or_fail":
                f = dlt.expect_or_fail(name, condition)(f)
            elif rule_type == "expect_all_or_drop":
                f = dlt.expect_all_or_drop({name: condition})(f)
            elif rule_type == "expect_all":
                f = dlt.expect_all({name: condition})(f)
        return f
    return decorator


from pyspark.sql.functions import lit, to_json, struct, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pyspark.sql.functions as F

def apply_dq(df: DataFrame, table_name: str, config_path: str):
    config = load_config(config_path)
    table_conf = config["tables"].get(table_name, {})

    if "cleansing" in table_conf:
        df = cleanse_columns(df, table_conf["cleansing"])

    if "drop_duplicates" in table_conf:
        df = drop_duplicates(
            df,
            table_conf["drop_duplicates"]["keys"],
            table_conf["drop_duplicates"].get("order_by")
        )

    valid_df = df
    invalid_dfs = []

    for rule in table_conf.get("expectations", []):
        clause = build_expectation_clause(rule)
        if rule.get("type") in ["expect_or_drop", "expect_all_or_drop"]:
            failed_df = df.filter(f"NOT ({clause})") \
                .withColumn("dq_table", lit(table_name)) \
                .withColumn("dq_failed_column", lit(rule["column"])) \
                .withColumn("dq_failed_rule", lit(rule["rule"])) \
                .withColumn("dq_reason", lit(clause)) \
                .withColumn("record_data", to_json(struct(*df.columns))) \
                .withColumn("logged_at", current_timestamp()) \
                .select("dq_table", "dq_failed_column", "dq_failed_rule", "record_data", "dq_reason", "logged_at")

            invalid_dfs.append(failed_df)

    return valid_df, invalid_dfs



