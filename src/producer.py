#Imports
import requests
import time
from datetime import datetime, timezone
import json
from kafka import KafkaProducer # New dependency

# --- CONFIGURATION ---

API_KEY = "cbf2293bada692a52db9c89027bfdf3f04357d31ca0b48498cbff438ab69a6ec"
HEADERS = {"X-API-Key": API_KEY}

# Producer configuration
LOOP_INTERVAL_SECONDS = 1 * 60

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']  # Kafka port
KAFKA_TOPIC = 'openaq_raw_readings'            # Target Kafka topic name

# Target Index.
TARGET_INDEX = 1
# Desired Parameter Label 
TARGET_PARAMETER_LABEL = "PM2.5 µg/m³"

# Consolidated list of global locations
STATION_IDS = [
 #Pakistan
 4568423, 4908792, 4848174, 4515644, 4808325, 4838122, 4952332,
 #India
 1236037, 2860223, 6003651, 4916349,
 #Bangladesh
 3194367,
 #South Korea
 3400997,
 # Global
 3481776, 6123215, 4902926, 4313237, 4878631, 2453499, 3458167, 3331918
]

# --- HELPER FUNCTION ---

def get_station_name(station_id, headers):
    """Fetches station name from OpenAQ v3 /locations/{id}."""
    url = f"https://api.openaq.org/v3/locations/{station_id}"
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [{}])[0].get("name", "Unknown")
    except requests.exceptions.RequestException:
        return "Unknown"

# --- CORE PRODUCER FUNCTION ---

def produce_raw_data_to_kafka(station_ids, headers, name_map, producer, scrape_time_utc):
    """
    Fetches the RAW list of sensor readings and sends the JSON payload to Kafka.
    """
    for loc_id in station_ids:
        url = f"https://api.openaq.org/v3/locations/{loc_id}/latest"
        station_name = name_map.get(loc_id)

        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            # CRITICAL CHANGE: Get the entire results list
            raw_readings = data.get('results', [])

            # Create the single, raw payload object
            payload = {
                'location_id': loc_id,
                'station_name': station_name,
                'scrape_time_utc': scrape_time_utc,
                'target_index': TARGET_INDEX, # Tell Spark which index to look at
                'parameter_label': TARGET_PARAMETER_LABEL,
                'raw_readings': raw_readings # Send the full raw list to Spark
            }

            # Send the payload to Kafka
            producer.send(KAFKA_TOPIC, payload)

        except requests.exceptions.RequestException as e:
            # Log API errors but continue the loop
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API ERROR for {station_name} ({loc_id}): {e}")
            continue

    producer.flush() # Ensure all messages are delivered
    return len(station_ids) # Return the count for logging

# --- MAIN PRODUCER LOOP ---

if __name__ == "__main__":

    # 1. Initialization
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10)
        )
    except Exception as e:
        print(f"FATAL: Could not initialize Kafka Producer. Check settings: {e}")
        exit(1)

    print("--- Initializing Air Quality Producer ---")
    print("Fetching static station names once...")
    name_map = {sid: get_station_name(sid, HEADERS) for sid in STATION_IDS}
    print(f"Initialization complete. Monitoring {len(STATION_IDS)} locations. Starting {LOOP_INTERVAL_SECONDS/60:.0f}-minute loop.")
    print("-" * 50)

    try:
        while True:
            start_time = time.time()

            # Prepare the universal scrape timestamp for this batch
            current_scrape_time = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

            # 1. Produce Data Batch
            record_count = produce_raw_data_to_kafka(
                STATION_IDS, HEADERS, name_map, producer, current_scrape_time
            )

            # Log current status
            scrape_time_display = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            print(f"\n[... BATCH SENT: {scrape_time_display}] - {record_count} raw records sent to Kafka topic '{KAFKA_TOPIC}'.")

            # 2. Wait for the Next Loop
            elapsed_time = time.time() - start_time
            time_to_wait = LOOP_INTERVAL_SECONDS - elapsed_time

            if time_to_wait > 0:
                print(f"Waiting {time_to_wait:.1f} seconds...")
                time.sleep(time_to_wait)
            else:
                print("Warning: Scrape took longer than the loop interval. Continuing immediately.")

    except KeyboardInterrupt:
        print("\nProducer stopped by user (Ctrl+C). Exiting.")
    finally:
        producer.close()