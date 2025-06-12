-- Databricks notebook source
/*
WE can built Delta live table using spark sql

in DLT, we use live keyword while create table dlt table. (here table is nothing but materialize view)

Types of tables in DLT:
1-> Streaming table
2-> Materialize view (table)
3-> View



*/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

/* sample code to create dlt table for csv file */

-- Create a temporary view from the CSV file
CREATE OR REPLACE TEMPORARY VIEW temp_csv_view
USING csv
OPTIONS (
  path '/path/to/your/csv/file',
  header 'true',
  inferSchema 'true'
);

-- Create a Delta Live Table from the temporary view
CREATE TABLE my_dlt_table
USING delta
AS SELECT * FROM temp_csv_view;

/*  another way */

-- Create a Delta Live Table directly from the CSV file
CREATE TABLE my_dlt_table
USING delta
LOCATION '/path/to/your/csv/file'
OPTIONS (
  header 'true',
  inferSchema 'true'
);

-- COMMAND ----------

/* Create a live table */ /* we can also use following to create a table "CREATE OR REFRESH MATERIALIZED VIEW sales (....) " */

CREATE LIVE TABLE <TABLE_NAME>
(

RideID        int    comment 'This is the primary key column'
,VendorId     int
,PickUpTime   Timestamp
,DropTime     Timestamp
,CabNumber    string
,TotalAmount  double 
,tripDistance int

,FileName     String
,CreatedOn    Timestamp
)

using delta

partitionBy(VenderId)   --here we puting partitionBy to generate file based on Vendor ID

comment 'Live bronze table for raw data'

As

select
* 
,INPUT_FILE_NAME() as FileName
,CURRENT_TIMESTAMP as CreatedON

from 
parquet.`<input file path>`  --or we can put delta table

-- COMMAND ----------

/* Create live silver table */

CREATE LIVE TABLE Silver_table
(
RideID        int    comment 'This is the primary key column'
,VendorId     int
,PickUpTime   Timestamp
,DropTime     Timestamp
,CabNumber    string
,TotalAmount  double 
,tripDistance int

--we can add calculated column like we do in Database

,PickupYear   INT         GENERATED ALWAYS AS (Year(PickUpTime))
,PickupMonth  INT         GENERATED ALWAYS AS (Month(PickUpTime))
,PickupDay   INT         GENERATED ALWAYS AS (Day(PickUpTime))


--we can create constrain for data quality checks

,CONSTRAINT <CONSTRAINT_NAME>      EXPECT (TotalAmount is not null and totalAmount> 0)  on violation drop row
,CONSTRAINT <CONSTRAINT_NAME>      EXPECT (tripDistance > 0)                            on violation drop row
,CONSTRAINT <CONSTRAINT_NAME>      EXPECT (RideID is not null and RideID> 0)            on violation fail update

)

using delta

partitionBy(PickUpTime)

comment '<Put some comments here>'

as
select * 
from live.<brozne_table>



-- COMMAND ----------

/* CREATE A SIMPLE VIEW  */

CREATE LIVE VIEW <VIEW_NAME>
(

  --IN VIEW ALSO WE CAN DEFINE CONSTRAINT
  CONSTRAINT <CONSTRAINT_NAME>      EXPECT (<CONDITION>)  on violation drop row
)
AS
SELECT * 
FROM LIVE.<TABLE_NAME>      --NOTE : IF IT IS DLT TABLE, THEN PUT LIVE BEFORE TABLE NAME

-- COMMAND ----------

/*  CREATING STREAMING TABLE (IT INCREMENTAL DATA) */

CREATE STREAMING LIVE TABLE
(
--DEFINE THE COLUMN WITH DATATYPES LIKE NORMALL TABLE
--we can create constrain fo data quality checks

,CONSTRAINT <CONSTRAINT_NAME>      EXPECT (TotalAmount is not null and totalAmount> 0)  on violation drop row 

)

using delta

partitionBy(PickUpTime)

comment '<Put some comments here>'

as
select 
/* WE CAN DEFINE THE COLUMN NAME AND IT'S DATATYPE HERE IF WE NOT DECLARING IT ABOVE */

RideId::Int, VendorId::int
......

from cloud_files( 
                  '<input file path>',
                  '<file extension like csv, parquet, json...>', 
                   map('inferSchema',True)
                )

/* here cloud_files mean autoloader, here whenever new file comes  it will get loaded.  */

-- COMMAND ----------

/* create streaming view, here we can see ony latest/new data whenever query run)  */

create streaming live view <view_name>
(
--IN VIEW ALSO WE CAN DEFINE CONSTRAINT
  CONSTRAINT <CONSTRAINT_NAME>      EXPECT (<CONDITION>)  on violation drop row

)
as
select 
<column_names>...

from

stream(live.<table_name>)

/* here we are using stream before table name since it streaming table */

-- COMMAND ----------

/* merge the two table changes/ data using 'apply changes' functionality */

APPLY CHANGES INTO LIVE.<TABLE_NAME> --TARGET TABLE

FROM STREAM(LIVE.<VIEW_NAME>)    --SOURCE TABLE / VIEW   (SINCE IT STREAMING VIEW, WE NEED TO PUT STREAM BEFORE TABLE/VIEW)

KEYS (RIDEID, VENDORID)

--APPLY AS DELETE WHEN OPERATION='DELETE'

SEQUENCE BY CREATEDON


-- COMMAND ----------

-- Apply changes into syntax :


-- create target streaming table :
CREATE OR REFRESH STREAMING TABLE target_table;

--apply chnage sinto statement :

APPLY CHANGES INTO target_table
FROM source_table
KEYS (primary_key_column)
SEQUENCE BY update_timestamp_column
[IGNORE NULL UPDATES]
[APPLY AS DELETE WHEN condition]
[APPLY AS TRUNCATE WHEN condition]
[COLUMNS * EXCEPT (column_list)]
[STORED AS {SCD TYPE 1 | SCD TYPE 2}]
[TRACK HISTORY ON * EXCEPT (column_list)];

