# Databricks notebook source
import dlt
from pyspark.sql.functions import col, coalesce, to_timestamp, lit, current_timestamp, trim
from pyspark.sql import functions as F
import json
import logging
from pyspark.sql.functions import col, upper, trim, regexp_replace, lower, date_format, to_timestamp, to_date, when, floor, date_diff, current_date, concat_ws, concat
from config import *
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table(
    name=f"{CATALOG}.{GOLD_DATABASE}.gold_org_monthly_metrics",
    comment="Monthly KPIs per organization",
    table_properties= {"pipelines.autoOptimize.zOrderCols" : "organization"}
)
def gold_org_monthly_metrics():
    encounters = (
        dlt.read_stream(f"{CATALOG}.{SILVER_DATABASE}.encounters").withColumn("start_ts", to_timestamp("start"))
        .withWatermark("start_ts", "30 days")
        .withColumn("encounter_month", date_format("start_ts", "yyyy-MM"))
    )
    orgs = dlt.read(f"{CATALOG}.{SILVER_DATABASE}.organizations")

    joined = (
        encounters.alias("e")
        .join(orgs.alias("o"), col("e.organization") == col("o.id"), "left")
    )

    return (
        joined
        .groupBy("organization", "o.name", "encounter_month")
        .agg(
            sum("e.total_claim_cost").alias("monthly_cost"),
            approx_count_distinct("e.patient").alias("unique_patients"),
            count("e.id").alias("total_visits"),
            avg("e.total_claim_cost").alias("avg_cost_per_visit")
        )
    )


# COMMAND ----------

@dlt.table(
    name=f"{CATALOG}.{GOLD_DATABASE}.gold_supply_utilization",
    comment="Supply usage and cost per encounter",
    table_properties= {"pipelines.autoOptimize.zOrderCols" : "encounter"}
    #table_properties={"delta.zorderCols": "encounter"}
)
def gold_supply_utilization():
    supplies = dlt.read_stream(f"{CATALOG}.{SILVER_DATABASE}.supplies").withColumn("supplie_date_ts", to_timestamp("supplie_date")).withWatermark("supplie_date_ts", "30 days")
    encounters = dlt.read(f"{CATALOG}.{SILVER_DATABASE}.encounters")

    enriched = (
        supplies.alias("s")
        .join(encounters.alias("e"), col("s.encounter") == col("e.id"), "left")
        .withColumn("month_up", date_format("s.supplie_date_ts", "yyyy-MM"))
    )

    return (
        enriched
        .groupBy("organization", "month_up")
        .agg(
            sum("s.quantity").alias("total_items_issued"),
            approx_count_distinct("s.encounter").alias("encounters_served")
        )
    )