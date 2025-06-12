# Databricks notebook source
# MAGIC %md
# MAGIC #Spark Architecture
# MAGIC The Spark follows the master-slave architecture. Its cluster consists of a single master and multiple slaves.
# MAGIC
# MAGIC ##The Spark architecture depends upon two abstractions:
# MAGIC
# MAGIC ->Resilient Distributed Dataset (RDD) 
# MAGIC
# MAGIC ->Directed Acyclic Graph (DAG) 
# MAGIC
# MAGIC ###Resilient Distributed Datasets (RDD) 
# MAGIC
# MAGIC The Resilient Distributed Datasets are the group of data items that can be stored in-memory on worker nodes. Here,
# MAGIC
# MAGIC ->Resilient: Restore the data on failure.
# MAGIC
# MAGIC -> Distributed: Data is distributed among different nodes.
# MAGIC
# MAGIC -> Dataset: Group of data.
# MAGIC
# MAGIC We will learn about RDD later in detail.
# MAGIC
# MAGIC ###Directed Acyclic Graph (DAG)
# MAGIC Directed Acyclic Graph is a finite direct graph that performs a sequence of computations on data. Each node is an RDD partition, and the edge is a transformation on top of data. Here, the graph refers the navigation whereas directed and acyclic refers to how it is done.
# MAGIC
# MAGIC #####Let's understand the Spark architecture.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ##Spark Architecture
# MAGIC
# MAGIC ###Driver Program
# MAGIC
# MAGIC The Driver Program is a process that runs the main() function of the application and creates the SparkContext object. The purpose of SparkContext is to coordinate the spark applications, running as independent sets of processes on a cluster.
# MAGIC
# MAGIC To run on a cluster, the SparkContext connects to a different type of cluster managers and then perform the following tasks: -
# MAGIC
# MAGIC ->It acquires executors on nodes in the cluster.
# MAGIC
# MAGIC ->Then, it sends your application code to the executors. Here, the application code can be defined by JAR or Python files passed to the SparkContext.
# MAGIC
# MAGIC ->At last, the SparkContext sends tasks to the executors to run.
# MAGIC
# MAGIC ### Cluster Manager
# MAGIC
# MAGIC ->The role of the cluster manager is to allocate resources across applications. The Spark is capable enough of running on a large number of clusters.
# MAGIC
# MAGIC ->It consists of various types of cluster managers such as Hadoop YARN, Apache Mesos and Standalone Scheduler.
# MAGIC
# MAGIC ->Here, the Standalone Scheduler is a standalone spark cluster manager that facilitates to install Spark on an empty set of machines.
# MAGIC
# MAGIC
# MAGIC ### Worker Node
# MAGIC
# MAGIC ->The worker node is a slave node
# MAGIC
# MAGIC ->Its role is to run the application code in the cluster.
# MAGIC
# MAGIC ###Executor
# MAGIC
# MAGIC ->An executor is a process launched for an application on a worker node.
# MAGIC
# MAGIC ->It runs tasks and keeps data in memory or disk storage across them.
# MAGIC
# MAGIC ->It read and write data to the external sources.
# MAGIC
# MAGIC ->Every application contains its executor.
# MAGIC
# MAGIC ### Task
# MAGIC
# MAGIC ->A unit of work that will be sent to one executor.
