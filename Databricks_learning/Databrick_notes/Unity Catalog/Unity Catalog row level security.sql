-- Databricks notebook source

use catalog test_dev;
use schema bronze;

-- COMMAND ----------

--create a table to implement row level security


create or replace table dev_row_level_security_tble
(
  id int
  ,Mail string 
  ,Dept string
)  --set row filter <function_name> on <dept(column name)>  (we can have row level filter in able create step itself)

-- COMMAND ----------

--alter the table for row level security

alter table dev_row_level_security_tble set row filter <function_name> on <dept(column name)>

-- COMMAND ----------

-- MAGIC % sql
-- MAGIC
-- MAGIC --implement a function to restrict the row for respective dept 
-- MAGIC
-- MAGIC create or replace function row_level_dept_healthcare(dept string)
-- MAGIC   return is_account_group_name('healthcare<group name>') and dept like '%<dept colunm data like healthcare>%'
-- MAGIC
-- MAGIC   /*
-- MAGIC     if user belongs to muliplt group then we have function like below : and when we query the table , then we get row which haivng both dept data
-- MAGIC     
-- MAGIC     create or replace function row_level_dept_healthcare(dept string)
-- MAGIC       return (is_account_group_name('healthcare') and dept like '%healthcare%') or 
-- MAGIC             (is_account_group_name('finance') and dept like '%finance%')
-- MAGIC   */

-- COMMAND ----------


--query the table to see only the user which belongs to above group and the dept data in table column having only dept healthcare

select * from dev_row_level_security_tble;
