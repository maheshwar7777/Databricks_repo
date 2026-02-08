# Databricks notebook source
import json
import time
import random
import datetime as dt
from typing import List, Dict, Optional
from kafka import KafkaProducer

# -------------------------------
# Kafka settings
# -------------------------------
KAFKA_BOOTSTRAP = "10.115.72.58:9092"
#KAFKA_TOPIC = "Live_feed_stream_uav"
KAFKA_TOPIC = "Live_feed_stream_uav_raw_src"

# -------------------------------
# Rate settings
# -------------------------------
BATCH_INTERVAL_SECONDS = 5  # same cadence as your original file

# -------------------------------
# Data generation settings
# -------------------------------
# Same Asset_ID logic/pattern as attached file: UAV_00001 ... UAV_00050
ASSET_IDS: List[str] = [f"UAV_{i:05d}" for i in range(1, 51)]  # UAV_00001..UAV_00050

# -------------------------------
# "Blank" behavior configuration (mirrors your attached code)
# -------------------------------
# Probability (0.0–1.0) for asset_id to be blank
PROB_BLANK_ASSET_ID = 0.05  # 5% chance asset_id blank
# Probability (0.0–1.0) for stream_url to be blank
PROB_BLANK_STREAM_URL = 0.05  # 5% chance stream_url blank
# What should "blank" be? Choose "null" (JSON null) or "empty" (empty string "")
BLANK_STYLE = "null"  # "null" or "empty"
# Composite key delimiter (same as attached: newline)
#KEY_DELIM = ""
KEY_DELIM = "#"

# -------------------------------
# Helpers (mirroring structure from your file)
# -------------------------------

def make_blank(value_type: str):
    """
    Returns the blank representation according to BLANK_STYLE.
    value_type: "string" or "number"
    """
    if BLANK_STYLE == "empty":
        # For strings -> "", for numbers -> "" (still string)
        return ""
    else:
        # "null" -> JSON null for both numbers & strings
        return None


def maybe_blank_string(s: str, probability: float) -> Optional[str]:
    return make_blank("string") if random.random() < probability else s


def current_timestamp_iso_z_ms() -> str:
    """
    Current UTC timestamp, ISO-8601 with millisecond precision and trailing 'Z',
    e.g., 2026-01-14T06:45:12.345Z
    """
    #now_utc = dt.datetime.now(dt.timezone.utc)  # timezone-aware UTC
    # isoformat() on aware datetimes ends with "+00:00"; convert to "Z"
    #return now_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")
   
    now_utc = dt.datetime.now(dt.timezone.utc)
    # Drop timezone info but keep microseconds
    now_utc = now_utc.replace(tzinfo=None)
    return now_utc.isoformat(timespec="microseconds")


# -------------------------------
# Event generator
# -------------------------------

def generate_event(asset_id: str) -> Dict:
    """
    Generates the stream-config JSON exactly as requested.
    - asset_id follows the same base logic as the attached file (UAV_00001..UAV_00050),
      then may be blanked based on PROB_BLANK_ASSET_ID and BLANK_STYLE.
    - stream_url may also be blanked based on PROB_BLANK_STREAM_URL and BLANK_STYLE.
    - asset_last_communication is the current timestamp (UTC, ms, 'Z').
    """
    now_iso = current_timestamp_iso_z_ms()

    # Apply "blank" probability to asset_id (same idea as your attached file)
    payload_asset_id = maybe_blank_string(asset_id, PROB_BLANK_ASSET_ID)

    # Base stream URL then apply blanking logic
    base_stream_url = "rtsp://uavcam.local:554/live"
    payload_stream_url = maybe_blank_string(base_stream_url, PROB_BLANK_STREAM_URL)

    event = {
        "asset_id": payload_asset_id,  # may be blank (null or "")
        "asset_type": "UAV",
        "asset_last_communication": now_iso,  # current timestamp in UTC
        "stream_protocol": "RTSP",
        "stream_url": payload_stream_url,  # may be blank (null or "")
        "resolution": "1920x1080",
        "fps": 30,
        "auth_type": "basic",
        "username": "uav_user",
        "password": "••••••",
    }
    return event

# -------------------------------
# Build composite Kafka key from asset_id and asset_last_communication
# -------------------------------

def build_composite_key(asset_id_payload: Optional[str], last_comm: str) -> str:
    """
    Returns a composite key "asset_id#asset_last_communication" (or with
    your configured delimiter). If asset_id is blank/null, we still include the
    delimiter so the pattern is consistent (mirrors your original approach).
    """
    left = "" if asset_id_payload in (None, "") else str(asset_id_payload)
    return f"{left}{KEY_DELIM}{last_comm}"

# -------------------------------
# Main loop
# -------------------------------

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(
        f"Started producer. Sending 50 messages every {BATCH_INTERVAL_SECONDS} seconds "
        f"to '{KAFKA_TOPIC}' on {KAFKA_BOOTSTRAP}. Ctrl+C to stop."
    )

    try:
        while True:
            # timezone-aware UTC start time
            batch_start = dt.datetime.now(dt.timezone.utc)

            for asset_id in ASSET_IDS:
                event = generate_event(asset_id)
                composite_key = build_composite_key(
                    event["asset_id"], event["asset_last_communication"]
                )
                # Send one message per asset (mirrors your original pattern)
                producer.send(KAFKA_TOPIC, key=composite_key, value=event)
                producer.flush()  # flush per message as in your attached file

            # Compute elapsed using timezone-aware UTC now for consistency
            elapsed = (dt.datetime.now(dt.timezone.utc) - batch_start).total_seconds()
            time.sleep(max(0, BATCH_INTERVAL_SECONDS - elapsed))

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")


if __name__ == "__main__":
    main()
