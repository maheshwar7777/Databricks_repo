# Databricks notebook source
# import libriraies

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,expr
from pyspark.sql import funcitons as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType, DoubleType

# COMMAND ----------

# create a schema for imoput csv file

schema = StructType([\
    StructField('col_name',StringType(),True),\
    StructField('col_name',StringType(),True),\
    StructField('col_name',StringType(),True),\
    StructField('col_name',StringType(),True),\
    .....
    ])

cloud_file_options = { 'CloudFile.format':'csv', 'header':True}


# COMMAND ----------

# create bronze table to store the raw data
# here we are creating streaming table

@dlt.table(name= '<Table_name>')

def <function_name>() :
    df=spark.readStream.format('cloudFiles').options(**cloud_file_options).schema(schema).load('<file_path>')

    #add some tranformations
    df.withColumn('File_process_data',f.date_format(f.current_timestamp(),'yyyy-mm-dd HH:mm:ss'))

    return df

# COMMAND ----------

#Expections or Quality checks defined

check={}

check["Validate circute col for null values <mention the dec related to quality check>"] = "(circuitId is not null)"
check["Validate name col for null valesu <mention th edec related to quality check>"] = "(Name is not null)"

# create string combines above checks conditions

dq_rules = "({0})".format('And '.join(checks.values()))


# COMMAND ----------

# create stage silver table while passed the quality checks

@dlt.table(name='stag_silver_load_circuit')

@dlt.expect_all(checks)   # here we have four type of quality checks. refer google to learn more

def stag_sile_table():
    df=dlt.readStream('brz_load_circuit')

    df=df.withColumn('dq_check',f.expr(dq_rules)).filter('dq_check=true')

    return df



# COMMAND ----------

#perform SCD type 1 by load traget table from source table

dlt.create_streaming_table (name = 'silver_load_circuit')

#here we use apply change method to transfer data from one table to another, it's like merge update
dlt.apply_changes(\
    target = 'silver_load_circuit',\
    source= 'stag_silver_load_circuit',\
    keys = ['circuitId'],\
    stored_as_scd_type = '1',\
    sequence_by = 'file_process_data'
    )

# COMMAND ----------

# capture th error records in another err table


@dlt.table(name='err_stag_load_circuit')

@dlt.expect_all(checks)

def err_silver_load_circuit():
    df=dlt.readStream('brz_load_circuit')

    df=df.withColumn('dq_check',f.expr(dq_rules)).filter('dq_check=false')

    return df

