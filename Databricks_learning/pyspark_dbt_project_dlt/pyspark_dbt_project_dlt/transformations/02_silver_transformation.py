# Apply transformation and clean the data

# importing lib 
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities.utils import *
from pyspark.sql.types import *

# Customers data

# create temp view
@dp.view
def v_customers():
    df=spark.readStream.table("pysparkdbt.bronze.customers")
    
    df= (
        df.withColumn('domain',split(col('email'), '@').getItem(1))
        .withColumn('Full_name', concat_ws(' ', col('first_name'), col('last_name')))
        .withColumn('phone_number', regexp_replace(col('phone_number'), '[^0-9]', ''))   
    )

    df = df.drop(col('first_name'), col('last_name'))

    obj_trans = Trans()
    df_trans_timestamp_update = obj_trans.process_timestamp(df)
    return df_trans_timestamp_update


# create silver table for customers
dp.create_streaming_table("pysparkdbt.silver.customers")

dp.create_auto_cdc_flow(
    target="pysparkdbt.silver.customers",
    source="v_customers",
    keys=["customer_id"],
    sequence_by=col("last_updated_timestamp")
)


# Drivers data

# create temp view
@dp.view
def v_drivers():
    df=spark.readStream.table("pysparkdbt.bronze.drivers")
    
    df= (
        df.withColumn('Full_name', concat_ws(' ', col('first_name'), col('last_name')))
        .withColumn('phone_number', regexp_replace(col('phone_number'), '[^0-9]', ''))   
    )

    df = df.drop(col('first_name'), col('last_name'))

    obj_trans = Trans()
    df_trans_timestamp_update = obj_trans.process_timestamp(df)
    return df_trans_timestamp_update


# create silver table for drivers
dp.create_streaming_table("pysparkdbt.silver.drivers")

dp.create_auto_cdc_flow(
    target="pysparkdbt.silver.drivers",
    source="v_drivers",
    keys=["driver_id"],
    sequence_by=col("last_updated_timestamp")
)


# Location data

# create temp view
@dp.view
def v_locations():
    df=spark.readStream.table("pysparkdbt.bronze.locations")

    obj_trans = Trans()
    df_trans_timestamp_update = obj_trans.process_timestamp(df)
    return df_trans_timestamp_update


# create silver table for locations
dp.create_streaming_table("pysparkdbt.silver.locations")

dp.create_auto_cdc_flow(
    target="pysparkdbt.silver.locations",
    source="v_locations",
    keys=["location_id"],
    sequence_by=col("last_updated_timestamp")
)

# Payment data

# create temp view
@dp.view
def v_payments():
    df=spark.readStream.table("pysparkdbt.bronze.payments")

    df= (
        df.withColumn('online_payment_status', 
                      when( ((col('payment_method')=='Card') & (col('payment_status')=='Success') ), lit('Online-success'))
                      .when(((col('payment_method')=='Card') & (col('payment_status')=='Failed') ), lit('Online-failed')) 
                      .when( ((col('payment_method')=='Card') & (col('payment_status')=='Pending') ), lit('Online-pending')) 
                      .otherwise('Offline')
                    )
    )

    obj_trans = Trans()
    df_trans_timestamp_update = obj_trans.process_timestamp(df)
    return df_trans_timestamp_update


# create silver table for payments
dp.create_streaming_table("pysparkdbt.silver.payments")

dp.create_auto_cdc_flow(
    target="pysparkdbt.silver.payments",
    source="v_payments",
    keys=["payment_id"],
    sequence_by=col("last_updated_timestamp")
)


# Trips data

# create temp view
@dp.view
def v_trips():
    df=spark.readStream.table("pysparkdbt.bronze.trips")

    obj_trans = Trans()
    df_trans_timestamp_update = obj_trans.process_timestamp(df)
    return df_trans_timestamp_update


# create silver table for trips
dp.create_streaming_table("pysparkdbt.silver.trips")

dp.create_auto_cdc_flow(
    target="pysparkdbt.silver.trips",
    source="v_trips",
    keys=["trip_id"],
    sequence_by=col("last_updated_timestamp")
)


# Vehicles data

# create temp view
@dp.view
def v_vehicles():
    df=spark.readStream.table("pysparkdbt.bronze.vehicles")

    df = df.withColumn('make',upper(col('make')))

    obj_trans = Trans()
    df_trans_timestamp_update = obj_trans.process_timestamp(df)
    return df_trans_timestamp_update


# create silver table for vehicles
dp.create_streaming_table("pysparkdbt.silver.vehicles")

dp.create_auto_cdc_flow(
    target="pysparkdbt.silver.vehicles",
    source="v_vehicles",
    keys=["vehicle_id"],
    sequence_by=col("last_updated_timestamp")
)