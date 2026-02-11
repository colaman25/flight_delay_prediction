import json
import time
import requests
import yaml
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os

# --- Load environment variables ---
load_dotenv()

API_KEY = os.getenv("FLIGHTAWARE_API")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "schedule-data")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "1800"))  # seconds

headers = {
    'x-apikey': API_KEY,
}

def load_config(config_path='config.yaml'):
    """Loads configuration from a YAML file."""
    with open(config_path, 'r') as file:
        # Use safe_load for security when working with configuration files
        config = yaml.safe_load(file)
    return config

def fetch_data_from_api(part_url):
    base_url = 'https://aeroapi.flightaware.com/aeroapi'
    try:
        response = requests.get(base_url+part_url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ERROR] API request failed: {e}")
        return None

def create_kafka_producer():
    """Initialize Kafka producer with JSON serialization."""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=100  # small buffer for batching
    )
    return producer


def run():
    """One-shot execution for Airflow."""
    # Try to create Kafka producer with a few retries
    retries = 5
    for attempt in range(1, retries + 1):
        try:
            producer = create_kafka_producer()
            break
        except NoBrokersAvailable:
            print(f"[WARN] Kafka not ready (attempt {attempt}/{retries}), retrying in 5s...")
            time.sleep(5)
    else:
        print("[ERROR] Kafka broker not available, exiting.")
        return

    # Load config
    config = load_config('config.yaml')
    data_cfg = config['data_config']
    airport_icao = data_cfg['schedule_airport']
    max_pages = data_cfg.get('max_pages', 1)

    part_url = f'/airports/{airport_icao}/flights/departures'
    print(f"✅ Collector started. Fetching from https://aeroapi.flightaware.com/aeroapi{part_url}")

    page = 1
    while page <= max_pages:
        data = fetch_data_from_api(part_url)
        if not data:
            print("⚠️ No data fetched this run.")
            break

        departures = data.get('departures') or data.get('scheduled_departures')
        if departures:
            for i, departure in enumerate(departures, start=1):
                producer.send(KAFKA_TOPIC, value=departure)
                print(f"📦 Sent message {i} on page {page} to Kafka topic '{KAFKA_TOPIC}'")

        # Prepare next page if available
        if 'links' in data and 'next' in data['links']:
            part_url = data["links"]["next"]
            page += 1
        else:
            print(f"📦 Reached last page.")
            break

        producer.flush()
        time.sleep(1)  # small delay between pages to avoid hitting rate limits

    producer.flush()
    print(f"✅ All messages sent to Kafka topic '{KAFKA_TOPIC}'")

if __name__ == "__main__":
    run()
