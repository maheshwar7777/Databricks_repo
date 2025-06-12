# Databricks notebook source
# MAGIC %md
# MAGIC ##  Joins 
# MAGIC
# MAGIC ### types of joins :
# MAGIC ##### keywords which needs to pass in:
# MAGIC Inner -> inner
# MAGIC
# MAGIC Left --> left , leftouter, left_outer
# MAGIC
# MAGIC Right --> right, rightouter, right_outer
# MAGIC
# MAGIC Full outer join --> Outer , Full, fullouter, full_outer
# MAGIC
# MAGIC cross join --> crossJoin
# MAGIC
# MAGIC Left Anti --> anti, leftanti, left_anti  (here : it displays all unmatched records from left dataframe (meaning : records which are not matched with right dataframe)  )
# MAGIC
# MAGIC left Semi --> semi, leftsemi, left_semi   (here : first it performs inner join and displays only left dataframe columns )

# COMMAND ----------

#  create two dataframes and join them 

df_1=spark.createDataFrame(data=[(1,'Mahesh'),(2,'Sam'),(3,'Reddy'),(5,('Jai'))],schema=['ID','Name'])
df_2=spark.createDataFrame(data=[(1,26),(2,34),(4,54)],schema=['ID','Age'])

display(df_1)
display(df_2)

# from pyspark.sql.types import *
# df_3 = spark.createDataFrame(range(10),IntegerType())

# display(df_3)


# COMMAND ----------

# using inner join 

df_fl=df_1.join(df_2,df_1.ID==df_2.ID,'inner')

display(df_fl)

# COMMAND ----------

# using Left join 

df_fl=df_1.join(df_2,df_1.ID==df_2.ID,'left')

display(df_fl)

# COMMAND ----------

# using right join 

df_fl=df_1.join(df_2,df_1.ID==df_2.ID,'right')

display(df_fl)

# COMMAND ----------

# using full join 

df_fl=df_1.join(df_2,df_1.ID==df_2.ID,'full')

display(df_fl)

# COMMAND ----------

# using cross join 

df_fl=df_1.crossJoin(df_2)

display(df_fl)

# COMMAND ----------

#  create two dataframes and join with two columns  

df_1=spark.createDataFrame(data=[(1,'Mahesh'),(2,'Sam'),(3,'Reddy'),(5,('Jai'))],schema=['ID','Name'])
df_2=spark.createDataFrame(data=[(1,'Mahesh',26),(2,'Sam',34),(4,'Reddy',54)],schema=['ID','Name','Age'])

display(df_1)
display(df_2)

# COMMAND ----------

# using inner join with two columns 

df_fl=df_1.join(df_2,(df_1.ID==df_2.ID) & (df_1.Name==df_2.Name),'inner').select(df_1.ID,df_1.Name,df_2.Age)

display(df_fl)

help(df_1.join(df_2))

# COMMAND ----------

# MAGIC %md
# MAGIC # Broadcast joins

# COMMAND ----------

# creating broadcast join using pyspark 

from pyspark.sql.functions import *

df_fl=df_1.join(broadcast(df_2),df_1.ID==df_2.ID,'inner')

display(df_fl)

# COMMAND ----------

# /* creating broadcast join in spark sql using sql hints. Here out small dim table in boarccast sql hint.*/

df_1.createOrReplaceTempView('df_one')

df_2.createOrReplaceTempView("df_two")

df_sql = spark.sql("""                 
  select *  /* broadcast(c) */
  from df_one as t left join df_two as c on t.ID = c.ID
       """)

df_sql.display()
