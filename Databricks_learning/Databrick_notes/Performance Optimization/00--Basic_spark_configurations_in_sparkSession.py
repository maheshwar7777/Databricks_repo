# Databricks notebook source
# creating spark session with some key spark configurations:

# importing libraries:

from pyspark.sql import SparkSession

spark = SparkSession.bulider. \
    config('spark.ui.port','0'). \    # I believe we dont use this configuration command in databricks
    appName('put spark creation name, like put purpose name'). \
    master('yarn'). \   # I believe we dont use this configuration command in databricks
    config('spark.executor.instances','2'). \   # we use this configuration to set no. of executors per worker node
    config('spark.executor.memory','4GB'). \   # we use this configuration to set memory sixe per executor in worker node
    config('spark.executor.cores','4'). \     # we use this configuration to set no. of cores per executor in worker node
    config('spark.dynamicAllocation.enabled','False'). \   # we use this configuration to disable dynamic allocation
    getorCreate() 
    
