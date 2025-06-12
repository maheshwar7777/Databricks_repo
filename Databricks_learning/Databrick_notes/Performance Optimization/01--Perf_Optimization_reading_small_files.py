# Databricks notebook source
'''

01- Scenario: Case when reading mulitple small files into dataframe, spark exectution takes long time.

Solution : While reading mulitple small file, we need to make sure minium 128 MB data stored in each parition, we can also perfomr some benmark testing to see how much data we can fit into each partition and find correct partition size.

'''

# COMMAND ----------

# use below command to check default partition size

spark.conf.get('spark.sql.files.maxPartitionBytes')

# we can set custom partition size using below command

spark.conf.set('spark.sql.files.maxPartitionBytes', 256 * 1024 * 1024)

# COMMAND ----------

# to read inoout file and stored intno dataframe

df=spark.read.format('csv').option('header',True).option('inferSchema',True).load('<input file path>')

# to perform write operation without writing into actual table/Path, use below format 'noop'


df.write.format("noop").mode("overwrite").save()


# COMMAND ----------

'''
Note : Information about 'noop' foramt from copilot:

--> In Databricks Spark, the 'noop' format stands for "no operation". It's a special format used primarily for testing and benchmarking Spark jobs without actually writing any data to storage.

Key Points about 'noop' format:
    --> No actual data is written: When you use .write.format("noop").save(), Spark goes through the motions of executing the job, but it    
        doesn't persist the output anywhere.
    --> Useful for benchmarking: It allows you to measure the performance of your transformations and actions without the overhead of writing 
        to disk or cloud storage.
    --> Triggers execution: Since Spark is lazy by default, using 'noop' helps force the execution of the job pipeline, which is useful for 
        debugging or performance profiling.

'''
