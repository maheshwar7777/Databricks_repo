from pyspark.sql.types import *
num_rows = 100000
catalog_name="rfp_batch_usecase"
bronze_schema_name="bronze"
silver_schema_name="silver"
gold_schema_name="gold"
volume_name="volumes"
output_base_path =  f"/Volumes/{catalog_name}/{bronze_schema_name}/{volume_name}"
bronze_tables_list=['observations','procedures','providers','medications','immunizations']
silver_tables_list=['observations','procedures','providers','medications','immunizations']
gold_tables_list=['medication_cost_summary_of_patients','medication_cost_summary','observation_trends','immunization_summary']
procedures=[]
immunizations=[]
medications=[]
providers=[]
observations=[]

procedures_schema = StructType([
    StructField("start_time", StringType(), False),
    StructField("stop_time", StringType(), True),
    StructField("patient", StringType(), False),
    StructField("encounter", StringType(), False),
    StructField("system", StringType(), False),
    StructField("code", StringType(), False),
    StructField("description", StringType(), False),
    StructField("base_cost", DoubleType(), False),
    StructField("reason_code", StringType(), True),
    StructField("reason_description", StringType(), True),
    StructField("part_year", StringType(), True),
    StructField("part_month", StringType(), True),
    StructField("part_date", StringType(), True)
])

providers_schema = StructType([
    StructField("id", StringType(), False),
    StructField("organization", StringType(), False),
    StructField("name", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("speciality", StringType(), False),
    StructField("address", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("encounters", DoubleType(), True),
    StructField("procedures", DoubleType(), True),
    StructField("part_year", StringType(), True),
    StructField("part_month", StringType(), True),
    StructField("part_date", StringType(), True)
])

observations_schema = StructType([
    StructField("observation_date", StringType(), False),
    StructField("patient", StringType(), False),
    StructField("encounter", StringType(), False),
    StructField("category", StringType(), True),
    StructField("code", StringType(), False),
    StructField("description", StringType(), False),
    StructField("value", StringType(), False),
    StructField("units", StringType(), True),
    StructField("type", StringType(), False),
    StructField("part_year", StringType(), True),
    StructField("part_month", StringType(), True),
    StructField("part_date", StringType(), True)
])

medications_schema = StructType([
    StructField("start_date", StringType(), True),
    StructField("stop_date", StringType(), True),
    StructField("patient", StringType(), True),
    StructField("payer", StringType(), True),
    StructField("encounter", StringType(), True),
    StructField("code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("base_cost", DoubleType(), True),
    StructField("payer_coverage", DoubleType(), True),
    StructField("dispenses", DoubleType(), True),
    StructField("totalcost", DoubleType(), True),
    StructField("reasoncode", StringType(), True),
    StructField("reasondescription", StringType(), True),
    StructField("part_year", StringType(), True),
    StructField("part_month", StringType(), True),
    StructField("part_date", StringType(), True)
])

immunizations_schema = StructType([
    StructField("immunization_date", StringType(), False),
    StructField("patient", StringType(), False),
    StructField("encounter", StringType(), False),
    StructField("code", StringType(), False),
    StructField("description", StringType(), False),
    StructField("base_cost", DoubleType(), False),
    StructField("part_year", StringType(), True),
    StructField("part_month", StringType(), True),
    StructField("part_date", StringType(), True)
])

table_schema_map={
    "procedures":procedures_schema,
    "providers":providers_schema,
    "observations":observations_schema,
    "medications":medications_schema,
    "immunizations":immunizations_schema}

tables_list_map={
    "procedures":procedures,
    "providers":providers,
    "observations":observations,
    "medications":medications,
    "immunizations":immunizations}

bronze_to_silver_mapping = {
    f"{catalog_name}.{bronze_schema_name}.immunizations":  f"{catalog_name}.{silver_schema_name}.immunizations",
    f"{catalog_name}.{bronze_schema_name}.medications":  f"{catalog_name}.{silver_schema_name}.medications",
    f"{catalog_name}.{bronze_schema_name}.observations":  f"{catalog_name}.{silver_schema_name}.observations",
    f"{catalog_name}.{bronze_schema_name}.procedures":  f"{catalog_name}.{silver_schema_name}.procedures",
    f"{catalog_name}.{bronze_schema_name}.providers":  f"{catalog_name}.{silver_schema_name}.providers"
}

bronze_silver_merge_keys = {
    f"{catalog_name}.{bronze_schema_name}.immunizations": ["immunization_date", "patient", "encounter", "code"],
    f"{catalog_name}.{bronze_schema_name}.medications": ["start_date", "patient", "payer", "encounter", "code"],
    f"{catalog_name}.{bronze_schema_name}.observations": ["observation_date", "patient", "encounter", "category", "code"],
    f"{catalog_name}.{bronze_schema_name}.procedures": ["start_time", "patient", "encounter", "system", "code"],
    f"{catalog_name}.{bronze_schema_name}.providers": ["id"]
}

# silver_to_gold_mapping = {
#     f"{catalog_name}.{silver_schema_name}.immunizations":  f"{catalog_name}.{gold_schema_name}.immunization_summary",
#     f"{catalog_name}.{silver_schema_name}.medications":  f"{catalog_name}.{gold_schema_name}.medication_cost_summary_of_patients",
#     f"{catalog_name}.{silver_schema_name}.observations":  f"{catalog_name}.{gold_schema_name}.observation_trends",
#     f"{catalog_name}.{silver_schema_name}.medications":  f"{catalog_name}.{gold_schema_name}.medication_cost_summary"
# }

silver_to_gold_mapping = [
    {
        "silver_table": f"{catalog_name}.{silver_schema_name}.immunizations",
        "gold_table": f"{catalog_name}.{gold_schema_name}.immunization_summary"
    },
    {
        "silver_table": f"{catalog_name}.{silver_schema_name}.medications",
        "gold_table": f"{catalog_name}.{gold_schema_name}.medication_cost_summary_of_patients"
    },
    {
        "silver_table": f"{catalog_name}.{silver_schema_name}.observations",
        "gold_table": f"{catalog_name}.{gold_schema_name}.observation_trends"
    },
    {
        "silver_table": f"{catalog_name}.{silver_schema_name}.medications",
        "gold_table": f"{catalog_name}.{gold_schema_name}.medication_cost_summary"
    }
]
