# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Data and Store in Volume for further Analysis

# COMMAND ----------

# MAGIC %run ./setup-entities

# COMMAND ----------

# MAGIC %pip install iso3166 Faker

# COMMAND ----------

# MAGIC %md
# MAGIC # Data generator for DLT pipeline
# MAGIC This notebook will generate data in the given storage path to simulate a data flow. 
# MAGIC
# MAGIC **Make sure the storage path matches what you defined in your DLT pipeline as input.**
# MAGIC
# MAGIC 1. Run Cmd 2 to show widgets
# MAGIC 2. Specify Storage path in widget
# MAGIC 3. "Run All" to generate your data
# MAGIC 4. When finished generating data, "Stop Execution"
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&org_id=2559555269299511&notebook=%2F_resources%2F00-Loan-Data-Generator&demo_name=dlt-loans&event=VIEW&path=%2F_dbdemos%2Fdata-engineering%2Fdlt-loans%2F_resources%2F00-Loan-Data-Generator&version=1">

# COMMAND ----------

from config import *

from faker import Faker
import random
import json
from datetime import datetime, timedelta
import uuid
import pyspark.sql.functions as F
from faker import Faker
from collections import OrderedDict 
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.types import datetime, TimestampType
from datetime import datetime, timedelta, date
import decimal
import uuid
import random
from decimal import Decimal
import pandas as pd
import random
import time
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

# COMMAND ----------

# DBTITLE 1,Run First for Widgets
dbutils.widgets.removeAll()

dbutils.widgets.combobox('reset_all_data', 'false', ['true', 'false'], 'Reset all existing data')
dbutils.widgets.combobox('batch_wait', '30', ['15', '30', '45', '60'], 'Speed (secs between writes)')
dbutils.widgets.combobox('num_recs', '500', ['20','100', '250', '500'], 'Volume (# records per writes)')
dbutils.widgets.combobox('batch_count', '1', ['1', '5','100', '200', '500'], 'Write count (how many times do we append data)')

catalog=CATALOG
raw_schema=RAW_DATABASE
volume_name=VOLUME_NAME


# COMMAND ----------

output_base_path =  f"/Volumes/{catalog}/{BASE_PATH}"
fake = Faker()


# COMMAND ----------

dbutils.fs.mkdirs(output_base_path)
reset_all_data = RESET_ALL_DATA == "true"


# COMMAND ----------


today = datetime.today()
def date_parts(dt):
    return dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d"), dt.strftime("%Y-%m-%d")


# COMMAND ----------

# # --- Generate Patients ---
# def generate_patient():
#     dob = fake.date_of_birth(minimum_age=18, maximum_age=90)
#     y, m, d, full = date_parts(today)
#     state = fake.state_abbr()
#     return {
#         "id": str(uuid.uuid4()),
#         "birthdate": str(dob),
#         "deathdate": None,
#         "ssn": fake.ssn(),
#         "drivers": fake.license_plate(),
#         "passport": fake.passport_number(),
#         "prefix": fake.prefix(),
#         "first": fake.first_name(),
#         "middle": fake.first_name(),
#         "last": fake.last_name(),
#         "suffix": fake.suffix(),
#         "maiden": fake.last_name(),
#         "marital": fake.random_element(["M", "S", "D"]),
#         "race": fake.random_element(["White", "Black", "Asian", "Hispanic"]),
#         "ethnicity": fake.random_element(["Not Hispanic", "Hispanic"]),
#         "gender": fake.random_element(["M", "F"]),
#         "birthplace": fake.city(),
#         "address": fake.street_address(),
#         "city": fake.city(),
#         "state": state,
#         "county": fake.city_suffix(),
#         "fips": str(fake.random_number(5, True)),
#         "zip": fake.zipcode(),
#         "lat": fake.latitude(),
#         "lon": fake.longitude(),
#         "healthcare_expenses": round(random.uniform(1000, 50000), 2),
#         "healthcare_coverage": round(random.uniform(500, 20000), 2),
#         "income": round(random.uniform(20000, 100000), 2),
#         "year": y,
#         "month": m,
#         "date": d,
#         "load_date": full
#     }

# COMMAND ----------


# # --- Generate Organizations ---
# def generate_organization(state=None):
#     y, m, d, full = date_parts(today)
#     state_abbr = state or fake.state_abbr()
#     return {
#         "id": str(uuid.uuid4()),
#         "name": fake.company(),
#         "address": fake.address().replace("\n", ", "),
#         "city": fake.city(),
#         "state": state_abbr,
#         "zip": fake.zipcode(),
#         "lat": fake.latitude(),
#         "lon": fake.longitude(),
#         "phone": fake.phone_number(),
#         "revenue": round(random.uniform(1e6, 1e8), 2),
#         "utilization": round(random.uniform(0.5, 1.0), 2),
#         "year": y,
#         "month": m,
#         "date": d,
#         "load_date": full
#     }

# # --- Generate Encounters ---
# def generate_encounter(patient, org_id):
#     birthdate = datetime.strptime(patient["birthdate"], "%Y-%m-%d")
#     start = birthdate + timedelta(days=random.randint(6570, 32850))  # age 18 to ~90
#     stop = start + timedelta(days=random.randint(1, 10))
#     y, m, d, full = date_parts(today)

#     return {
#         "id": str(uuid.uuid4()),
#         "start": str(start),
#         "stop": str(stop),
#         "patient": patient["id"],
#         "organization": org_id,
#         "provider": fake.name(),
#         "payer": fake.company(),
#         "encounterclass": fake.random_element(["inpatient", "outpatient", "emergency"]),
#         "code": fake.bothify(text="E####"),
#         "description": fake.catch_phrase(),
#         "base_encounter_cost": round(random.uniform(500, 5000), 2),
#         "total_claim_cost": round(random.uniform(1000, 10000), 2),
#         "payer_coverage": round(random.uniform(1000, 9000), 2),
#         "reasoncode": fake.lexify(text="RC???"),
#         "reasondescription": fake.sentence(nb_words=4),
#         "year": y,
#         "month": m,
#         "date": d,
#         "load_date": full
#     }


# COMMAND ----------



fake = Faker()

today = datetime.today()

def date_parts(ref_date):
    y, m, d = ref_date.year, ref_date.month, ref_date.day
    return y, m, d, ref_date.strftime("%Y-%m-%d")

def generate_patient(bad=False):
    if bad:
        # Inject some intentionally bad data
        birthdate = fake.future_date(end_date="+30d")  # future birthdate (invalid)
        deathdate = fake.date_between(start_date="-30y", end_date="today")  # past deathdate
        gender = random.choice(["X", "unknown", None])  # invalid gender
        ssn = "bad_ssn_value"
        income = -random.uniform(1000, 5000)  # invalid income
    else:
        birthdate = fake.date_of_birth(minimum_age=18, maximum_age=90)
        deathdate = None
        gender = fake.random_element(["M", "F"])
        ssn = fake.ssn()
        income = round(random.uniform(20000, 100000), 2)

    y, m, d, full = date_parts(today)
    state = fake.state_abbr()

    return {
        "id": str(uuid.uuid4()),
        "birthdate": str(birthdate),
        "deathdate": str(deathdate) if deathdate else None,
        "ssn": ssn,
        "drivers": fake.license_plate(),
        "passport": fake.passport_number() if not bad else None,  # force null for invalid
        "prefix": fake.prefix(),
        "first": fake.first_name(),
        "middle": fake.first_name(),
        "last": fake.last_name(),
        "suffix": fake.suffix(),
        "maiden": fake.last_name(),
        "marital": fake.random_element(["M", "S", "D"]),
        "race": fake.random_element(["White", "Black", "Asian", "Hispanic"]),
        "ethnicity": fake.random_element(["Not Hispanic", "Hispanic"]),
        "gender": gender,
        "birthplace": fake.city(),
        "address": fake.street_address(),
        "city": fake.city(),
        "state": state,
        "county": fake.city_suffix(),
        "fips": str(fake.random_number(5, True)),
        "zip": fake.zipcode(),
        "lat": fake.latitude(),
        "lon": fake.longitude(),
        "healthcare_expenses": round(random.uniform(1000, 50000), 2),
        "healthcare_coverage": round(random.uniform(500, 20000), 2),
        "income": income,
        "year": y,
        "month": m,
        "date": d,
        "load_date": full
    }


def generate_organization(state=None, bad=False):
    y, m, d, full = date_parts(today)
    state_abbr = state or fake.state_abbr()
    revenue = -1000 if bad else round(random.uniform(1e6, 1e8), 2)
    utilization = 101.5 if bad else round(random.uniform(0.5, 1.0), 2)

    return {
        "id": str(uuid.uuid4()),
        "name": fake.company(),
        "address": fake.address().replace("\n", ", "),
        "city": fake.city(),
        "state": state_abbr,
        "zip": fake.zipcode(),
        "lat": fake.latitude(),
        "lon": fake.longitude(),
        "phone": fake.phone_number(),
        "revenue": revenue,
        "utilization": utilization,
        "year": y,
        "month": m,
        "date": d,
        "load_date": full
    }


def generate_encounter(patient, org_id, bad=False):
    birthdate = datetime.strptime(patient["birthdate"], "%Y-%m-%d")
    start = birthdate + timedelta(days=random.randint(6570, 32850))
    stop = start + timedelta(days=random.randint(1, 10))
    y, m, d, full = date_parts(today)

    if bad:
        base_encounter_cost = -random.uniform(500, 1000)
        total_claim_cost = random.uniform(25000, 50000)
        encounterclass = "random_class"
    else:
        base_encounter_cost = round(random.uniform(500, 5000), 2)
        total_claim_cost = round(random.uniform(1000, 10000), 2)
        encounterclass = fake.random_element(["inpatient", "outpatient", "emergency"])

    return {
        "id": str(uuid.uuid4()),
        "start": str(start),
        "stop": str(stop),
        "patient": patient["id"],
        "organization": org_id,
        "provider": fake.name(),
        "payer": fake.company(),
        "encounterclass": encounterclass,
        "code": fake.bothify(text="E####"),
        "description": fake.catch_phrase(),
        "base_encounter_cost": base_encounter_cost,
        "total_claim_cost": total_claim_cost,
        "payer_coverage": round(random.uniform(1000, 9000), 2),
        "reasoncode": fake.lexify(text="RC???"),
        "reasondescription": fake.sentence(nb_words=4),
        "year": y,
        "month": m,
        "date": d,
        "load_date": full
    }


# COMMAND ----------

def write_csv_records(path, records):
    pdf = pd.DataFrame(records)
    df = spark.createDataFrame(pdf)
    if "patients" in path:
        df = df.withColumn("deathdate", F.col("deathdate").cast("string"))
    df.write.mode("overwrite").option("header", "true").csv(path)
    print("data inserted at", path)
    print(df.count())

# COMMAND ----------


batch_count = BATCH_COUNT
assert batch_count <= 500, "please don't go above 500 writes, the generator will run for a too long time"


# COMMAND ----------

# --- MAIN Execution (with Bad Data Injection) ---



NUM_PATIENTS = NUMBER_OF_RECORDS
ENCOUNTERS_PER_PATIENT = random.randint(2, 5)  # One-to-many
BAD_DATA_RATIO = 0.1  # 10% bad data

patients = []
orgs_by_state = {}
orgs = []
encounters = []

for i in range(0, batch_count):
    if batch_count > 1:
        time.sleep(BATCH_WAIT)

    for _ in range(NUM_PATIENTS):
        inject_bad = random.random() < BAD_DATA_RATIO
        patient = generate_patient(bad=inject_bad)
        state = patient["state"]
        patients.append(patient)

        # Create 1 org per state if not already
        if state not in orgs_by_state:
            org_bad = random.random() < BAD_DATA_RATIO
            org = generate_organization(state, bad=org_bad)
            orgs.append(org)
            orgs_by_state[state] = org["id"]

        org_id = orgs_by_state[state]

        # Create encounters for this patient (some clean, some bad)
        for _ in range(ENCOUNTERS_PER_PATIENT):
            encounter_bad = random.random() < BAD_DATA_RATIO
            encounters.append(generate_encounter(patient, org_id, bad=encounter_bad))

    # Write all data
    write_csv_records(f"{output_base_path}patients", patients)
    write_csv_records(f"{output_base_path}organizations", orgs)
    write_csv_records(f"{output_base_path}encounters", encounters)

    print(f'Finished writing batch: {i+1}/{batch_count}')