# Databricks notebook source
'''

What is RDD , dataframe and datasets
  1-> They all are the API's provided by spark for developers for data processing and analytics.
  2-> In terms of functionality , all are same and returns same output for provided input data. But they differr in the way of handling and processing data, so there is difference in terms of performance, user convenencs and language support etc.
  3->user can choice any of the API while working with spark.

RDD (Resilient distributed dataset)
  1-> it was introducted in spark 1.0 in 2011

Dataframe:
  1-> it was introducted in spark 1.3 in 2013

Dataset:
  1-> it was introducted in spark 1.6 in 2015

Best of RDD: programming control (OOPs) and type safety
best of dataframe : Relational format , optimization and memory management

Dataset == Best of RDD + best of dataframe


Similiarites of above API's :
  1-> Fault tolerant (While working with spark and execute certain tranformation or commands, it wont process the task, it just prepares the logical plans and  based on it constructs the lineage graph (DAG - Directed acyclic gragh). Basically it keeps all the flow and use it logic gragh to construct the data again when any failure's happens.

  2-> Distributed in nature
  3-> In memory parallel processing
  4-> immutable
  5-> Lazy evaluation
  6-> Internally processing as RDD's for API's.

'''
