# from databricks.sdk.runtime import spark

# drop schema to cleanup sample data
def cleanup_data(spark, catlg, db):

    # delete catalog, sample data
    # spark.sql("DROP CATALOG if exists `{CATALOG}` cascade")
    # in future if required to keep catalog and delete schemas
    # spark.sql("DROP SCHEMA if exists `{CATALOG}`.`{DLT_META_DATABASE}` cascade")
    spark.sql(f"""DROP SCHEMA if exists {catlg}.{db} cascade""")
    print("Schema dropped successfully")


