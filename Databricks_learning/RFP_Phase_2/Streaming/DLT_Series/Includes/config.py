import re
from databricks.sdk.runtime import *

# Get current Databricks username to isolate catalog
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)


# Catalog & Schema Definitions
CATALOG           = f"DLT_demo_{clean_username}"   # Unity Catalog catalog name (unique per user)
RAW_DATABASE      = "raw_schema"                   # Used for raw data 
BRONZE_DATABASE   = "rfp2_bronze"                  # Bronze layer schema
SILVER_DATABASE   = "rfp2_silver"                  # Silver layer schema
GOLD_DATABASE     = "rfp2_gold"                    # Gold layer schema


# Volume & Data Paths
VOLUME_NAME       = "sample_data"                                                    # Unity Catalog volume name
BASE_PATH         = f"{RAW_DATABASE}/{VOLUME_NAME}/raw_data/"     # Raw data input path
SCHEMA_PATH       = f"{RAW_DATABASE}/{VOLUME_NAME}/infer_schema/" # Schema inference path


# Pipeline Notebook & Table list
PIPELINE_NOTEBOOK = "01-Python for Delta Live Tables"                               # Path to the DLT logic notebook
TABLE_LIST        = ["encounters", "organizations", "patients"]  # Datasets to process


# Data generation paramater
RESET_ALL_DATA     = "False"   # If True, deletes all previously generated data
NUMBER_OF_RECORDS  = 1000      # Number of records per batch
BATCH_WAIT         = 30        # Seconds to wait between batches
BATCH_COUNT        = 1         # Number of batches to generate


# Data Quality (DQ) Config for Silver Layer
# DQ_INPUT defines validation rules for specific tables in Silver layer.
# Each rule includes:
# - a unique name (e.g., "id_not_null")
# - a type: how the rule behaves (e.g., expect, expect_or_drop, expect_or_fail)
# - a condition: SQL expression used to validate the rule
DQ_INPUT = '''{
  "tables": {
    "patients": {
      "validations": {
        "id_not_null": {
          "type": "expect",
          "condition": "id IS NOT NULL"
        },
        "passport_not_null": {
          "type": "expect",
          "condition": "passport IS NOT NULL"
        },
        "birthdate_past": {
          "type": "expect",
          "condition": "birthdate < CURRENT_DATE()"
        },
        "deathdate_future": {
          "type": "expect",
          "condition": "deathdate > CURRENT_DATE()"
        },
        "gender_in_list": {
          "type": "expect_or_drop",
          "condition": "gender IN ('F', 'other')"
        },
        "race_in_list": {
          "type": "expect",
          "condition": "LOWER(race) IN ('white', 'black', 'asian', 'native', 'hispanic', 'other')"
        },
        "ethnicity_in_list": {
          "type": "expect",
          "condition": "LOWER(ethnicity) IN ('hispanic', 'non-hispanic')"
        },
        "ssn_regex": {
          "type": "expect",
          "condition": "ssn RLIKE '^[0-9]{3}-[0-9]{2}-[0-9]{4}$'"
        },
        "income_range": {
          "type": "expect_or_drop",
          "condition": "income BETWEEN 0 AND 100000"
        },
        "healthcare_expenses_range": {
          "type": "expect",
          "condition": "healthcare_expenses BETWEEN 0 AND 500000"
        },
        "healthcare_coverage_range": {
          "type": "expect",
          "condition": "healthcare_coverage >= 0"
        }
      }
    },
    "encounters": {
      "validations": {
        "base_encounter_cost_range": {
          "type": "expect_or_drop",
          "condition": "base_encounter_cost BETWEEN 0 AND 10000"
        },
        "total_claim_cost_range": {
          "type": "expect_or_drop",
          "condition": "total_claim_cost BETWEEN 0 AND 20000"
        },
        "encounterclass_in_list": {
          "type": "expect",
          "condition": "encounterclass IN ('inpatient', 'outpatient', 'ambulatory', 'emergency')"
        }
      }
    },
    "organizations": {
      "validations": {
        "revenue_range": {
          "type": "expect_or_drop",
          "condition": "revenue BETWEEN 0 AND 100000000"
        },
        "utilization_range": {
          "type": "expect",
          "condition": "utilization BETWEEN 0 AND 100"
        }
      }
    }
  }
}'''







