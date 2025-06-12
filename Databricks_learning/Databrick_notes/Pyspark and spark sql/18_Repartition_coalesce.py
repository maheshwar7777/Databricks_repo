# Databricks notebook source
# repartition : 

'''
Repartition : The repartition method is used to increase or decrease the number of partitions in a DataFrame. It involves a full shuffle of the data, making it an expensive operation.

When to Use :
Repartitioning is commonly used when you want to increase the parallelism of your computation by dividing the data into more partitions. This can be beneficial for operations that can be performed in parallel, such as map and filter.

repartitionByRange() :
The repartitionByRange() function in Spark is used to repartition a DataFrame based on a specified range of values from a column. This can be particularly useful when you want to control the distribution of data across partitions based on the values of a specific column.

Coalesce() :
In contrast, the Coalesce method offers a more optimized operation. It reduces the number of partitions without a full shuffle, resulting in lower computational overhead compared to Repartition. However, it's important to note that this method may lead to uneven data distribution.

When to use:
Use Coalesce when decreasing the number of partitions to reduce computational overhead. It is efficient for coalescing data into a smaller number of partitions without a full shuffle.

'''


# checking csv file size:

display(dbutils.fs.ls('dbfs:/FileStore/tables/DBFiles/raw_data.csv'))





# COMMAND ----------

# to check the default parallel partitions

display(sc.defaultParallelism)


# COMMAND ----------

# to check the default memory size for each partition :

display(spark.conf.get('spark.sql.files.maxPartitionBytes'))

# COMMAND ----------

# read the csv file and check the no of partitions.  here sinze size of below csv file is below default size (128 mb),  it created only one partition

df = spark.read.format('csv').option('header',True).option('inferschema', True).load('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

df.rdd.glom().collect()   # here without repartitions and coalesce funtion applied to dataframe

# COMMAND ----------

#let create the repartitions for the same file

df = spark.read.format('csv').option('header',True).option('inferschema', True).load('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

df_1 = df.repartition(10)    # here we applying repartition for currect dataframe  (i mean we are overwriting default partitions)

df_1.rdd.getNumPartitions()  # here  df_1.rdd.getNumPartitions()  used to display the number of partitions for respectice dataframe

df_1.rdd.glom().collect()    # here glom() is used to diaply the data flowing in which partitions



# COMMAND ----------

#coalesce function:

df = spark.read.format('csv').option('header',True).option('inferschema', True).load('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

df_1 = df.coalesce(2)    # here we applying coalesce for currect dataframe  (i mean we are overwriting default partitions)

df_1.rdd.getNumPartitions()   # here  df_1.rdd.getNumPartitions()  used to display the number of partitions for respectice dataframe

df_1.rdd.glom().collect()    # here glom() is used to diaply the data flowing in which partitions



# COMMAND ----------

# let increase the partitions by more than defualt  partitions and see :

df = spark.read.format('csv').option('header',True).option('inferschema', True).load('dbfs:/FileStore/tables/DBFiles/raw_data.csv')

df_1 = df.coalesce(1)    # here we applying coalesce for currect dataframe  (i mean we are overwriting default partitions)

df_1.rdd.getNumPartitions()   # here  df_1.rdd.getNumPartitions()  used to display the number of partitions for respectice dataframe

df_1.rdd.glom().collect()    # here glom() is used to diaply the data flowing in which partitions


# here we seeing default partitions after applying coalesce functions.


