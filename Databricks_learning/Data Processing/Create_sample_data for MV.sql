-- Databricks notebook source
CREATE OR REPLACE TABLE data_streaming.default.sales_data (
  sale_date DATE,
  region STRING,
  sale_amount DOUBLE
);


-- COMMAND ----------

INSERT INTO data_streaming.default.sales_data VALUES
  ('2025-09-01', 'North', 1500.00),
  ('2025-09-01', 'South', 1200.50),
  ('2025-09-01', 'North', 800.00),
  ('2025-09-02', 'East', 950.75),
  ('2025-09-02', 'South', 1100.00),
  ('2025-09-03', 'West', 1300.25),
  ('2025-09-03', 'North', 700.00);

-- COMMAND ----------

CREATE FUNCTION data_streaming.default.row_filter() RETURN IS_MEMBER('FinanceTeam');

-- COMMAND ----------

CREATE FUNCTION data_streaming.default.column_mask(sale_amount DOUBLE) 
RETURN IF(IS_MEMBER('FinanceTeam'), sale_amount, NULL);

-- COMMAND ----------

SELECT * FROM data_streaming.default.sales_summary_sample_mv limit 10;

-- COMMAND ----------

SELECT * FROM data_streaming.default.sales_summary_mv2 limit 10;

-- COMMAND ----------

SELECT * FROM data_streaming.default.sensitive_sales_mv limit 10;