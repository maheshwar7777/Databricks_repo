-- Databricks notebook source
--Create delta table

create table if not exists Grains
(
  name string
  ,color string
  ,grams float
  ,delicious boolean
)
;

-- COMMAND ----------

--- populate the table with sample table

insert into grains values ('Rice','white',1,True)

insert into grains values ('Wheat','brown',3,True),('Soya','brown',5,True),('Dal','yellow',1,True),('corn','yellow',2,True)

-- COMMAND ----------

-- fetch the meta data of the table

desc extended Grains ;

-- COMMAND ----------

-- fetch the hist of the table

desc history grains;

-- COMMAND ----------

--update the table for time tavel and rollback the change

delete from grains where name='Rice';

-- COMMAND ----------

select * from grains;

-- COMMAND ----------

desc history grains;

-- COMMAND ----------

-- now rollback to vrison 2 changes

restore table grains to version as of 2;

-- COMMAND ----------

select * from  grains

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #chect the delta tabes files in location
-- MAGIC
-- MAGIC display(dbutils.fs.ls('/user/hive/warehouse/grains/'))
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #create new dataframe and add a columns 
-- MAGIC
-- MAGIC df = spark.sql("select current_date as date, * from  grains ")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)
-- MAGIC
-- MAGIC df.write.format('delta').mode('overwrite').saveAsTable('new_grains')

-- COMMAND ----------

select * from new_grains

-- COMMAND ----------


--optimize the files

optimize new_grains
zorder by date


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/new_grains'))
