# Databricks notebook source
# sample syntax for merge update (upsert)

"""
MERGE INTO product AS target
USING updated_products_Data AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
UPDATE SET target.price = source.price
WHEN NOT MATCHED THEN
INSERT (product_id, product_name, price)
VALUES (source.product_id, source.product_name, source.price);
"""

# COMMAND ----------



df_trg = '<target data frame>'

df_source = '<source data frame>'

# sample merge update in pyspark

df_trg.alias('tgt').merge(
    source = df_source.alias('src')
    ,condition = "tgt.id=src.id and tgt.name=src.name"
).whenMatchedUpdate(set=
                    {
                        "tgt.age" : "src.age"
                        ,"tgt.addreses" : "src.address"

                    }
).whenNotMatchedInsert(values=
                       {
                           "tgt.id" : "src.id"
                           ,"tgt.name" : "src.name"
                           ,"tgt.age" : "src.age"
                           ,"tgt.addreses" : "src.address"
                       }

).execute()


