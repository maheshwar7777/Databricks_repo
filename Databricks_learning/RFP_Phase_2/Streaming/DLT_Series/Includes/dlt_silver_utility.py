from pyspark.sql.functions import col, to_date, to_timestamp, lit, to_json, struct, current_timestamp
from Includes.config import *
from pyspark.sql.functions import *
import dlt


def preprocess_derived_columns(df, config):
    if "derived_columns" in config:
        for new_col, expression in config["derived_columns"].items():
            df = df.withColumn(new_col, expr(expression))
    return df

def apply_cleanup_functions(df, config):
    if "drop_duplicates" in config:
        subset = config["drop_duplicates"].get("subset", [])
        df = df.dropDuplicates(subset)

    if "datetime_standardization" in config:
        date_fmt = config["datetime_standardization"].get("date_format", "yyyy-MM-dd")
        ts_fmt = config["datetime_standardization"].get("timestamp_format", "yyyy-MM-dd HH:mm:ss")
        for column in df.columns:
            if "date" in column.lower():
                df = df.withColumn(column, col(column).cast(StringType()))

    if "fillna" in config:
        df = df.fillna(config["fillna"])

    if "trim_columns" in config:
        for colname in config["trim_columns"]:
            df = df.withColumn(colname, trim(col(colname)))

    if "value_mapping" in config:
        for col_name, mapping in config["value_mapping"].items():
            expr_col = when(lit(False), None)
            for from_val, to_val in mapping.items():
                if from_val != "default":
                    expr_col = expr_col.when(col(col_name) == from_val, lit(to_val))
            expr_col = expr_col.otherwise(mapping.get("default", col(col_name)))
            df = df.withColumn(col_name, expr_col)

    if "derived_columns" in config:
        for new_col, expression in config["derived_columns"].items():
            df = df.withColumn(new_col, expr(expression))

    if "concat_columns" in config:
        for new_col, rule in config["concat_columns"].items():
            cols = [regexp_replace(col(c), rule.get("clean_regex", ""), "") for c in rule["columns"]]
            df = df.withColumn(new_col, concat_ws(rule.get("separator", " "), *cols))

    if "drop_columns" in config:
        df = df.drop(*config["drop_columns"])

    return df

    
# ----------------------------------
# Function: Apply Expectations Dynamically
# ----------------------------------
def apply_expectations(expectation_config):
    def decorator(f):
        for name, rule in reversed(expectation_config.items()):
            rule_type = rule.get("type", "expect_or_drop")
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









