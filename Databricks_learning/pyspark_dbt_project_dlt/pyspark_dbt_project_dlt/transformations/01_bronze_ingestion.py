from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities.utils import *
from pyspark.sql.types import *


# create entities list
entities = ['customers', 'drivers', 'locations', 'payments', 'trips', 'vehicles']
format = 'csv'


# Ingect source data into bronze tables dynamically
for entity in entities:
    @dp.table(
        name=f"pysparkdbt.bronze.{entity}",
        comment=f"Creating bronze table for {entity}"
    )
    def make_table(entity=entity):
        df_sh = (
            spark.read.format('csv')
            .option("header", "true")
            .option("inferSchema", "true")
            .load(f"/Volumes/pysparkdbt/source/vl_source_data/{entity}/*.csv")
        )
        df_schema = df_sh.schema

        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", format)
            .option("header", "true")
            .schema(df_schema)
            .option("cloudFiles.schemaLocation", f"/Volumes/pysparkdbt/bronze/vl_bronze/checkpoint/{entity}")
            .load(f"/Volumes/pysparkdbt/source/vl_source_data/{entity}/*.csv")
            .withColumn("ingest_time", current_timestamp())
            .withColumn("input_file_name", col("_metadata.file_name"))
        )
        
        return df
