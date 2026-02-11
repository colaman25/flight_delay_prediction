import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

# --- Load environment variables ---
load_dotenv()

API_URL = os.getenv("API_URL", "https://opensky-network.org/api/states/all")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flight-data")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "900"))  # seconds
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

STATE_FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude",
    "squawk", "spi", "position_source"
]

def get_access_token():
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    response = requests.post(TOKEN_URL, data=data)
    response.raise_for_status()  # throws error if not 200 OK

    return response.json()["access_token"]

def to_structured_state(raw_state):
    return {STATE_FIELDS[i]: raw_state[i] if i < len(raw_state) else None
            for i in range(len(STATE_FIELDS))}


def create_kafka_producer():
    """Initialize Kafka producer with JSON serialization."""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=100  # small buffer for batching
    )
    return producer

def fetch_data_from_api():
    """Fetch JSON data from the external API."""
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ERROR] API request failed: {e}")
        return None

def run():
    """Main one-shot execution for Airflow."""
    producer = create_kafka_producer()
    data = fetch_data_from_api()

    if not data or 'states' not in data:
        print("⚠️ No data fetched this run.")
        return

    states = data['states']
    if isinstance(states, list):
        for i, state in enumerate(states, start=1):
            structured = to_structured_state(state)
            producer.send(KAFKA_TOPIC, value=structured)
            print(f"📦 Sent message {i} to Kafka topic '{KAFKA_TOPIC}'")
    else:
        producer.send(KAFKA_TOPIC, value=states)
        print(f"📦 Sent single message to Kafka topic '{KAFKA_TOPIC}'")

    producer.flush()
    print(f"✅ All messages sent to Kafka topic '{KAFKA_TOPIC}'")

if __name__ == "__main__":
    run()
