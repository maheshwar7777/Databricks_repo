import random
from datetime import datetime, timedelta
import json
import os
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, substring, col, current_timestamp
from Includes.config import *
fake = Faker()

def create_spark_session():
    spark = SparkSession.builder.appName("RFP_Batch_usecase").getOrCreate()
    return spark

def random_datetime(start_year=2020, end_year=2025):
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    return fake.date_time_between(start_date=start_date, end_date=end_date)
    
def date_parts(dt):
    return dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d"), dt.strftime("%Y-%m-%d")

def write_json_files(output_path, records):
    with open(output_path, "w") as json_file:
        json.dump(records, json_file, indent=4)
    print(f"JSON file created successfully at {output_path}")

def generate_procedures():
    start_time = random_datetime()
    stop_time = start_time + timedelta(minutes=random.randint(10, 1000)) if random.random() > 0.3 else None
    reason_code=fake.bothify(text="RC-###") if random.random() > 0.2 else None
    y, m, d, full = date_parts(start_time)
    return{
            "start_time": str(start_time),
            "stop_time":str(stop_time),
            "patient": fake.uuid4(),
            "encounter": fake.uuid4(),
            "system": random.choice(["ICD-10", "SNOMED", "LOINC"]),
            "code": fake.bothify(text="??-###"),
            "description": fake.sentence(nb_words=4),
            "base_cost": round(random.uniform(100, 10000), 2),
            "reason_code":str(reason_code),
            "reason_description": fake.sentence(nb_words=5) if reason_code else None,
            "part_year": y,
            "part_month": m,
            "part_date": full
            }
    
def generate_immunizations():
        immunization_date = fake.date_between(start_date='-3y', end_date='today')
        y,m,d,full=date_parts(immunization_date)
        return{
            "immunization_date": str(immunization_date),
            "patient": fake.uuid4(),
            "encounter": fake.uuid4(),
            "code": fake.bothify(text="IMM-###"),
            "description": random.choice([
                "COVID-19 Vaccine", "Influenza Vaccine", "Hepatitis B", 
                "Tetanus", "MMR", "Varicella", "HPV", "Rabies Vaccine"
            ]),
            "base_cost": round(random.uniform(25, 300), 2),
            "part_year": y,
            "part_month": m,
            "part_date": full
            }
    
def generate_providers():
    today=datetime.today()
    y, m, d, full = date_parts(today)
    return{
            "id": fake.uuid4(),
            "organization": fake.company(),
            "name": fake.name(),
            "gender": random.choice(["Male", "Female", "Other"]),
            "speciality": random.choice(["Cardiology", "Oncology", "Neurology", "Pediatrics", "General Medicine"]),
            "address": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
            "lat": float(fake.latitude()),
            "lon": float(fake.longitude()),
            "encounters": round(random.uniform(50, 1000), 2),
            "procedures": round(random.uniform(10, 500), 2),
            "part_year": y,
            "part_month": m,
            "part_date": full
            }

def generate_observations():
    observation_types = ["vital-sign", "lab", "clinical", "imaging"]
    categories = ["vital-signs", "laboratory", "diagnosis", "procedure"]
    units_pool = ["mmHg", "bpm", "mg/dL", "°C", "kg/m²"]
    observation_date=fake.date_between(start_date="-2y", end_date="today")
    y,m,d,full=date_parts(observation_date)
    return{
        "observation_date": str(observation_date),
        "patient": fake.uuid4(),
        "encounter": fake.uuid4(),
        "category": random.choice(categories),
        "code": fake.bothify(text="OBS-###"),
        "description": fake.sentence(nb_words=3),
        "value": str(round(random.uniform(1, 200), 2)),
        "type": random.choice(["SNOMED", "LOINC", "RxNorm"]),
        "units": random.choice(units_pool),
        "obs_type": random.choice(observation_types),
        "part_year": y,
        "part_month": m,
        "part_date": full
        }

def generate_medications():
    start_date = fake.date_between(start_date='-2y', end_date='today')
    stop_date = start_date + timedelta(days=random.randint(1, 90)) if random.random() > 0.3 else None
    y,m,d,full=date_parts(start_date)
    base_cost=float(round(random.uniform(10.0, 500.0), 2))
    dispenses=round(random.uniform(1, 5), 0)
    reasoncode=fake.bothify("RC-###") if random.random() > 0.2 else None
    return{
        "start_date": str(start_date),
        "stop_date": str(stop_date),
        "patient": fake.uuid4(),
        "payer": random.choice(['Aetna', 'UnitedHealth', 'Cigna', 'BlueCross', 'Medicare', 'Medicaid']),
        "encounter": fake.uuid4(),
        "code": fake.bothify("MED-###"),
        "description": fake.word().capitalize() + " medication",
        "base_cost": base_cost,
        "payer_coverage": round(base_cost * random.uniform(0.4, 0.9), 2),
        "dispenses": dispenses,
        "totalcost": round(base_cost * dispenses, 2),
        "reasoncode": reasoncode,
        "reasondescription": fake.sentence(nb_words=5) if reasoncode else None,
        "part_year": y,
        "part_month": m,
        "part_date": full
    }

def load_new_files_bronze(table_name,batch_count,output_base_path):
    
    assert batch_count <= 100000, "please don't go above 100000 writes, the generator will run for a too long time"
    
    data_list=tables_list_map[table_name]

    for i in range(batch_count):
        if table_name=="procedures":
            data_list.append(generate_procedures())
        elif table_name=="providers":
            data_list.append(generate_providers())
        elif table_name=="observations":
            data_list.append(generate_observations())
        elif table_name=="medications":
            data_list.append(generate_medications())
        elif table_name=="immunizations":
            data_list.append(generate_immunizations())

    folder_date=f"""{datetime.now().year}{datetime.now().month:02d}{datetime.now().day:02d}"""

    file_timestamp=f"""{datetime.now().year}{datetime.now().month:02d}{datetime.now().day:02d}{datetime.now().hour:02d}{datetime.now().minute:02d}{datetime.now().second:02d}"""

    os.makedirs(f"{output_base_path}/{table_name}/{folder_date}/", exist_ok=True)

    write_json_files(f"{output_base_path}/{table_name}/{folder_date}/{table_name}_{file_timestamp}.json", data_list)

def readfrombronzeDF(bronze_table):
    spark=create_spark_session()
    df=spark.table(f"{bronze_table}").withColumn('ingestiontime',to_timestamp(substring(col('ingestedfilepath'),-19,14),'yyyyMMddHHmmss'))
    df = df.drop(*['_change_type', '_commit_version', '_commit_timestamp','year', 'month','date', 'load_date', 'ingestedfilepath'])
    return df

def readfromsilverDF(silver_table):
    spark=create_spark_session()
    df=spark.table(f"{silver_table}").withColumn('ingestiontime',current_timestamp())
    df = df.drop(*['_change_type', '_commit_version', '_commit_timestamp','year', 'month','date', 'load_date'])
    return df

