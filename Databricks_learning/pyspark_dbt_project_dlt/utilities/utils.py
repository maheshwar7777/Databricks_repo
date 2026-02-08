from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from typing import List
# from pyspark.sql.functions import udf
# from pyspark.sql.types import FloatType


# @udf(returnType=FloatType())
# def distance_km(distance_miles):
#     """Convert distance from miles to kilometers (1 mile = 1.60934 km)."""
#     return distance_miles * 1.60934

# create transformation class

class Trans:

    # def dedup(self, df:DataFrame, dedup_cols:List, cdc:str):
    #     df= df.withColumn("dedupKey", concat(*dedup_cols))
    #     df = df.withColumn("dedupCount", row_number().over(Window.partitionBy("dedupKey").orderBy(col(cdc).desc())))
    #     df = df.filter("dedupCount = 1")
    #     df = df.drop("dedupKey", "dedupCount")

    #     return df
    
    def process_timestamp(self, df:DataFrame):
        df = df.withColumn("process_timestamp", current_timestamp())

        return df
    

