CATALOG = "rfp2_streaming"  
BRONZE_DATABASE = "rfp2_bronze"  
SILVER_DATABASE = "rfp2_silver"  
GOLD_DATABASE = "rfp2_gold"
BASE_PATH = "abfss://usecase-impl-db@dbpracticejetsa.dfs.core.windows.net/landing-zone/"
SCHEMA_PATH = "/Volumes/rfp_streaming_phase2/default/schema_files/"
TABLE_LIST = ["allergies", "conditions", "encounters", "organizations", "supplies", "patients"]
AUTOLOADER_BRONZE_CHECKPOINT_PATH = "abfss://usecase-impl-db@dbpracticejetsa.dfs.core.windows.net/bronze/checkpoints/"

DQ_INPUT = '''{
  "tables": {
    "patients": {
      "cleanup_functions": {
        "drop_duplicates": {
          "subset": ["id"]
        },
        "fillna": {
          "county": "NA",
          "fips": "NA",
          "zip": "NA",
          "lat": 0,
          "lon": 0
        },
        "trim_columns": ["first", "last", "middle", "ssn", "gender", "marital"],

        "value_mapping": {
          "gender": {
            "M": "male",
            "F": "female",
            "default": "UNKNOWN"
          },
          "marital": {
            "S": "single",
            "M": "married",
            "D": "divorced",
            "W": "widowed",
            "default": "UNKNOWN"
          }
        },
        "derived_columns": {
          "deathindicator": "IF(deathdate IS NULL, 'Alive', 'Deceased')",
          "age": "FLOOR(DATEDIFF(CURRENT_DATE(), birthdate)/365.25)"
        },
        "concat_columns": {
          "patientname": {
            "columns": ["first", "middle", "last"],
            "separator": " ",
            "clean_regex": "[^a-zA-Z]"
          }
        },
        "drop_columns": ["deathdate", "prefix", "suffix", "maiden"]
      },
      "validations": {
        "ssn_col_is_null": {
          "type": "expect_or_drop",
          "condition": "ssn IS NOT NULL"
        },
        "age_non_negative": {
          "type": "expect_or_fail",
          "condition": "age >= 0"
        }
      }
    },
    "allergies": {
      "cleanup_functions": {
        "trim_columns": ["description", "description1", "description2"],
        "regex_replace_columns": {
          "code": "[^a-zA-Z0-9]"
        },
        "fillna": {
          "stop": "9999-12-31",
          "type": "NA",
          "category": "NA",
          "reaction1": "NA",
          "severity1": "NA",
          "reaction2": "NA",
          "severity2": "NA"
        }
      },
      "validations": {
        "description_col_is_null": {
          "type": "expect_or_drop",
          "condition": "description IS NOT NULL"
        },
        "stop_before_start_is_invalid": {
          "type": "expect",
          "condition": "stop IS NULL OR stop >= start"
        }
      }
    }
  }
}'''







