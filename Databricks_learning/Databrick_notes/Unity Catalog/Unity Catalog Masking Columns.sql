-- Databricks notebook source

use catalog test_dev;
use schema bronze;

-- COMMAND ----------

 -- create a function to mask the data for non specified groups or memeber 

 create or replace function masking_column(input string)
        return if (is_account_group_member('<Group name>'),input,'********')

-- COMMAND ----------

-- MAGIC % sql
-- MAGIC --create a table with mask column
-- MAGIC
-- MAGIC create or replace table dev_mask_col_tble
-- MAGIC (
-- MAGIC   id int
-- MAGIC   ,Mail string mask masking_column
-- MAGIC )

-- COMMAND ----------


--insert the data into able table and add member to any group and see if non mentioned group memeber seeing the column data in abvoe column.
