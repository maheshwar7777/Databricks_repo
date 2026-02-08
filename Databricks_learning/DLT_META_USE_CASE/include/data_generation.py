# Databricks notebook source
# MAGIC %md
# MAGIC ### Run Setup utilities   

# COMMAND ----------

# MAGIC %run ./setup-entities

# COMMAND ----------

# %pip install iso3166 Faker
# %restart_python


# COMMAND ----------

from faker import Faker
import csv
import datetime
# from config import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import random
import pandas as pd
import os


# %load_ext autoreload
# %autoreload 2

# COMMAND ----------

# creating faker object
fake = Faker()

# Generating customers , products and transaction data

# customers data
def generate_customer_data(num_records):
    customer_ids = [fake.random_number(digits=10) for _ in range(num_records)]
    data = []
    for customer_id in customer_ids:
        data.append({
            'customer_id': customer_id,
            'name': fake.name(),
            'email': fake.email(),
            'phone_number': fake.phone_number(),
            'address': fake.address().replace('\n', ' '),
            'created_at': fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'),
            'Op': 'I'
        })
    return data, customer_ids

# product data
def generate_product_data(num_records):
    product_ids = [fake.random_number(digits=10) for _ in range(num_records)]
    data = []
    for product_id in product_ids:
        data.append({
            'product_id': product_id,
            'product_name': fake.word(),
            'category': fake.word(),
            'price': fake.random_number(digits=5),
            'stock_quantity': fake.random_number(digits=3),
            'created_at': fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'),
            'Op': 'I'
        })
    return data, product_ids

# Transaction data
def generate_transaction_data(num_records, customer_ids, product_ids):
    data = []
    for _ in range(num_records):
        data.append({
            'transaction_id': fake.random_number(digits=10),
            'customer_id': random.choice(customer_ids),
            'product_id': random.choice(product_ids),
            'quantity': fake.random_number(digits=2),
            'total_price': fake.random_number(digits=5),
            'transaction_date': fake.date_time_this_decade().date(),
            'created_at': fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'),
            'Op': 'I'
        })
    return data


# COMMAND ----------

# Function to create sample scv files

def write_to_csv(data, filename):
    if data:
        keys = data[0].keys()
        dir_path = os.path.dirname(filename)
        os.makedirs(dir_path, exist_ok=True)
        with open(filename, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)

# COMMAND ----------


# Generate sample csv files

def generate_incremental_files(input_entity_cust:str =None , input_entity_prod:str =None, input_entity_trans:str =None, row_cnt:int =100):
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    customer_data, customer_ids = generate_customer_data(row_cnt)
    product_data, product_ids = generate_product_data(row_cnt)
    transaction_data = generate_transaction_data(row_cnt, customer_ids, product_ids)

    if input_entity_cust == 'customer':
        # customer_data = generate_customer_data(row_cnt)
        customer_file = f"{BASE_PATH}/customer/customer_{timestamp}.csv"
        write_to_csv(customer_data, customer_file)
        print("Sample customer file created")


    if input_entity_prod == 'product':
        # product_data = generate_product_data(row_cnt)
        product_file = f"{BASE_PATH}/product/product_{timestamp}.csv"
        write_to_csv(product_data, product_file)
        print("Sample product file created")


    if input_entity_trans == 'transaction':
        # transaction_data = generate_transaction_data(row_cnt)
        transaction_file = f"{BASE_PATH}/transaction/transaction_{timestamp}.csv"
        write_to_csv(transaction_data, transaction_file)
        print("Sample transaction file created")




# COMMAND ----------

# run below fucntion to generate sample csv files
# expected parameters are customer, product, transaction and number of rows

# generate_incremental_files(input_entity_cust='customer', input_entity_prod='product', input_entity_trans='transaction', row_cnt=300)