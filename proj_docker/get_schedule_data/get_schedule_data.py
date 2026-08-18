import json
import time
import requests
import yaml
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import MetadataEmptyBrokerList
import os

# --- Load environment variables ---
load_dotenv()

API_KEY = os.getenv("FLIGHTAWARE_API")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
DEPARTURE_KAFKA_TOPIC = os.getenv("DEPARTURE_KAFKA_TOPIC", "departure-schedule-data")
ARRIVAL_KAFKA_TOPIC = os.getenv("ARRIVAL_KAFKA_TOPIC", "arrival-schedule-data")
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

def fetch_data_from_api(part_url, max_retries=5, default_backoff=10):
    base_url = 'https://aeroapi.flightaware.com/aeroapi'
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(base_url+part_url, headers=headers, timeout=10)
            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", default_backoff))
                print(f"[WARN] Rate limited (429), waiting {wait}s before retry ({attempt}/{max_retries})...")
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"[ERROR] API request failed: {e}")
            return None
    print(f"[ERROR] Still rate limited after {max_retries} retries, giving up on this request.")
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


def collect_flights(producer, airport_icao, max_pages, direction, kafka_topic):
    """Fetch paginated flight data for one direction (departures/arrivals) and send it to Kafka."""
    part_url = f'/airports/{airport_icao}/flights/{direction}'
    print(f"✅ Collector started. Fetching from https://aeroapi.flightaware.com/aeroapi{part_url}")

    page = 1
    while page <= max_pages:
        data = fetch_data_from_api(part_url)
        if not data:
            print("⚠️ No data fetched this run.")
            break

        flights = data.get(direction) or data.get(f"scheduled_{direction}")
        if flights:
            for i, flight in enumerate(flights, start=1):
                producer.send(kafka_topic, value=flight)
                print(f"📦 Sent message {i} on page {page} to Kafka topic '{kafka_topic}'")

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
    print(f"✅ All messages sent to Kafka topic '{kafka_topic}'")


def run():
    """One-shot execution for Airflow."""
    # Try to create Kafka producer with a few retries
    retries = 5
    for attempt in range(1, retries + 1):
        try:
            producer = create_kafka_producer()
            break
        except MetadataEmptyBrokerList:
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

    collect_flights(producer, airport_icao, max_pages, "departures", DEPARTURE_KAFKA_TOPIC)
    collect_flights(producer, airport_icao, max_pages, "arrivals", ARRIVAL_KAFKA_TOPIC)

if __name__ == "__main__":
    run()
