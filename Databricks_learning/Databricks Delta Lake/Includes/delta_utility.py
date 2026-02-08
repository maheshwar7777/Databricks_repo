from databricks.sdk.runtime import *
from .config import *

def print_delta_config():
    print("Delta Tables Training Configuration:")
    print(f"Catalog: {CATALOG}")
    print(f"Schema: {DEMO_SCHEMA}")
    print(f"Volume: {VOLUME_NAME}")

def cleanup_delta_demo():
    try:
        spark.sql(f"DROP CATALOG IF EXISTS `{CATALOG}` CASCADE")
        print("Cleanup completed successfully!")
    except Exception as e:
        print(f"Cleanup error: {e}")