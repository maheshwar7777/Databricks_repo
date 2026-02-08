# Databricks notebook source

import json
import random
import time
import datetime as dt
from typing import List, Dict, Optional

from kafka import KafkaProducer

# --------------------------
# Kafka settings
# --------------------------
KAFKA_BOOTSTRAP = "10.115.72.58:9092"
KAFKA_TOPIC = "Kafka-UAV-sample-data"
#KAFKA_TOPIC = "Kafka-UAV-sample-data-stream-test"

# --------------------------
# Rate settings
# --------------------------
BATCH_INTERVAL_SECONDS = 10  # set to 120 for 2 minutes

# --------------------------
# Data generation settings
# --------------------------
ASSET_IDS: List[str] = [f"UAV_{i:05d}" for i in range(1, 51)]
STATUSES = ["Active", "Idle", "Offline"]
PAYLOAD_TYPES = ["Camera", "Radar", "Medical Kit", "Ammo"]
WEATHER_RESISTANCE = ["Low", "Medium", "High"]
MANUFACTURERS = ["AlphaTech", "BetaDynamics", "DeltaCorp"]
REGIONS = [f"Region_{i}" for i in range(1, 5)]

# --------------------------
# "Blank" behavior configuration
# --------------------------
# Probability (0.0–1.0) for each field to be blank
PROB_BLANK_ASSET_ID = 0.05         # 5% chance Asset_ID blank
PROB_BLANK_BATTERY = 0.10          # 10% chance Battery blank
PROB_BLANK_SIGNAL_STRENGTH = 0.10  # 10% chance Signal Strength blank
PROB_BLANK_TEMPERATURE = 0.08      # 8% chance Temperature blank

# What should "blank" be? Choose "null" (JSON null) or "empty" (empty string "")
BLANK_STYLE = "null"  # "null" or "empty"

# Composite key delimiter
KEY_DELIM = "|"

def make_blank(value_type: str):
    """
    Returns the blank representation according to BLANK_STYLE.
    value_type: "string" or "number"
    """
    if BLANK_STYLE == "empty":
        # For strings -> "", for numbers -> "" (still string)
        # (Downstream may need to parse/handle this.)
        return ""
    else:
        # "null" -> JSON null for both numbers & strings
        return None

def maybe_blank_string(s: str, probability: float) -> Optional[str]:
    return make_blank("string") if random.random() < probability else s

def maybe_blank_number(n: float, probability: float):
    return make_blank("number") if random.random() < probability else n

# --------------------------
# Event generator
# --------------------------
def generate_event(asset_id: str) -> Dict:
    utc_now_iso = dt.datetime.utcnow().isoformat()

    # Potentially blank Asset_ID in payload
    payload_asset_id = maybe_blank_string(asset_id, PROB_BLANK_ASSET_ID)

    # Punjab, India bounding box (approximate)
    PUNJAB_LAT_MIN, PUNJAB_LAT_MAX = 29.30, 32.32
    PUNJAB_LON_MIN, PUNJAB_LON_MAX = 73.55, 76.50

    event = {
        "Asset_ID": payload_asset_id,
        "Mission_ID": f"M{random.randint(1, 99):04d}",
        "Operation_Group_ID": f"OG{random.randint(1, 20):03d}",
        "Region_ID": random.choice(REGIONS),

        #"Latitude": round(random.uniform(-90.0, 90.0), 6),
        #"Longitude": round(random.uniform(-180.0, 180.0), 6),

        # Constrained to Punjab
        "Latitude": round(random.uniform(PUNJAB_LAT_MIN, PUNJAB_LAT_MAX), 6),
        "Longitude": round(random.uniform(PUNJAB_LON_MIN, PUNJAB_LON_MAX), 6),

        "Heading_deg": round(random.uniform(0.0, 360.0), 2),
        "Velocity_Vector": round(random.uniform(0.0, 150.0), 2),

        "Status": random.choice(STATUSES),

        # These may be blank based on probabilities
        "Battery_Level_percentage": maybe_blank_number(
            round(random.uniform(1.0, 100.0), 2),
            PROB_BLANK_BATTERY
        ),
        "Signal_Strength_dBm": maybe_blank_number(
            round(random.uniform(-90.0, -30.0), 2),
            PROB_BLANK_SIGNAL_STRENGTH
        ),
        "Temperature_C": maybe_blank_number(
            round(random.uniform(-20.0, 50.0), 2),
            PROB_BLANK_TEMPERATURE
        ),

        "Payload_Type": random.choice(PAYLOAD_TYPES),
        "Payload_Weight_kg": round(random.uniform(0.5, 50.0), 2),

        "Humidity_percentage": round(random.uniform(0.0, 100.0), 2),
        "Weather_Resistance": random.choice(WEATHER_RESISTANCE),

        "Firmware_Version": f"v{random.randint(2,5)}.{random.randint(0,9)}",
        "Manufacturer": random.choice(MANUFACTURERS),

        "Last_Communication": utc_now_iso,
        "Altitude_m": round(random.uniform(0.0, 1000.0), 2),
        "Category": "UAV",
    }
    return event

# --------------------------
# Build composite Kafka key from Asset_ID and Last_Communication
# --------------------------
def build_composite_key(asset_id_payload: Optional[str], last_comm: str) -> str:
    """
    Returns a composite key "Asset_ID|Last_Communication".
    If Asset_ID is blank/null, we still include the delimiter so the pattern is consistent.
    """
    left = "" if asset_id_payload in (None, "") else str(asset_id_payload)
    return f"{left}{KEY_DELIM}{last_comm}"

# --------------------------
# Main loop
# --------------------------
def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Started producer. Sending 50 messages every {BATCH_INTERVAL_SECONDS} seconds to '{KAFKA_TOPIC}' on {KAFKA_BOOTSTRAP}. Ctrl+C to stop.")

    try:
        while True:
            batch_start = dt.datetime.utcnow()
            for asset_id in ASSET_IDS:
                event = generate_event(asset_id)

                # Composite key uses the *payload* Asset_ID (which may be blank) and Last_Communication.
                composite_key = build_composite_key(event["Asset_ID"], event["Last_Communication"])

                # Send
                producer.send(KAFKA_TOPIC, key=composite_key, value=event)

            producer.flush()
            elapsed = (dt.datetime.utcnow() - batch_start).total_seconds()
            time.sleep(max(0, BATCH_INTERVAL_SECONDS - elapsed))
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()
