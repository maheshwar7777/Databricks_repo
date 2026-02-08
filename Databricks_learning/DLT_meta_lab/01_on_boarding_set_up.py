# Databricks notebook source
# from include.config import *

# COMMAND ----------

# Create bronze and silver dataflowspec tables

from pyspark.sql.types import *

# bronze dataflowspec schema

data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "schema",
            "partitionColumns",
            "cdcApplyChanges",
            "applyChangesFromSnapshot",
            "dataQualityExpectations",
            "quarantineTargetDetails",
            "quarantineTableProperties",
            "appendFlows",
            "appendFlowsSchemas",
            "clusterBy",
            "version",
            "createDate",
            "createdBy",
            "updateDate",
            "updatedBy"
        ]
data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField(
                    "sourceDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "readerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("targetFormat", StringType(), True),
                StructField(
                    "targetDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "tableProperties", MapType(StringType(), StringType(), True), True
                ),
                StructField("schema", StringType(), True),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
                StructField("applyChangesFromSnapshot", StringType(), True),
                StructField("dataQualityExpectations", StringType(), True),
                StructField(
                    "quarantineTargetDetails",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField(
                    "quarantineTableProperties",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("appendFlows", StringType(), True),
                StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
                StructField("clusterBy", ArrayType(StringType(), True), True),
                StructField("version", StringType(), True),
                StructField("createDate", TimestampType(), True),
                StructField("createdBy", StringType(), True),
                StructField("updateDate", TimestampType(), True),
                StructField("updatedBy", StringType(), True),
            ]
        )

df=spark.createDataFrame([],data_flow_spec_schema)
df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{DLT_META_DATABASE}.bronze_specs")
print("Bronze dataflow spec table is created")

# Silver schema :
sil_data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "partitionColumns",
            "cdcApplyChanges",
            "dataQualityExpectations",
            "appendFlows",
            "appendFlowsSchemas",
            "clusterBy",
            "selectExp",
            "whereClause",
            "version",
            "createDate",
            "createdBy",
            "updateDate",
            "updatedBy"
        ]
sil_data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField(
                    "sourceDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "readerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("targetFormat", StringType(), True),
                StructField(
                    "targetDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "tableProperties", MapType(StringType(), StringType(), True), True
                ),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
                StructField("dataQualityExpectations", StringType(), True),
                StructField("appendFlows", StringType(), True),
                StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
                StructField("clusterBy", ArrayType(StringType(), True), True),
                StructField("selectExp", ArrayType(StringType(), True), True),
                StructField("whereClause", ArrayType(StringType(), True), True),
                StructField("version", StringType(), True),
                StructField("createDate", TimestampType(), True),
                StructField("createdBy", StringType(), True),
                StructField("updateDate", TimestampType(), True),
                StructField("updatedBy", StringType(), True),
            ]
        )
    
df_1 = spark.createDataFrame([],sil_data_flow_spec_schema)
df_1.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{DLT_META_DATABASE}.silver_specs")

print("Silver dataflow spec table is created")

# COMMAND ----------

# %pip install dlt-meta 

# COMMAND ----------

# Register metadata in bronze and silver dataflowspec tables

from src.onboard_dataflowspec import OnboardDataflowspec

onboarding_params_map = {
            "database": f"{CATALOG}.{DLT_META_DATABASE}",
            "onboarding_file_path": f"{ON_BOARDING_JSON}/on_boarding_json_file.json",
            "bronze_dataflowspec_table": "bronze_specs",
            "silver_dataflowspec_table": "silver_specs",
            "overwrite": "true",
            "env": "prod",
            "version": "v1",
            "import_author": "Maheshwar",
        }




OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_dataflow_specs()

# COMMAND ----------


# -- query bronze dataflowspec table
display(spark.sql(f"""select * from {CATALOG}.{DLT_META_DATABASE}.bronze_specs"""))

# COMMAND ----------

# -- query silver dataflowspec table

display(spark.sql(f"""select * from {CATALOG}.{DLT_META_DATABASE}.silver_specs"""))