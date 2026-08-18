# Lambda port of proj_docker/get_flight_data/get_flight_data.py.
#
# Fetches OpenSky state vectors and publishes them to SQS instead of
# directly to Kafka -- a separate, VPC-attached Lambda (publish_to_kafka)
# consumes this queue and produces to MSK. This Lambda deliberately stays
# outside the VPC so it keeps free internet access to call the OpenSky API
# (MSK Serverless has no path in from outside the VPC, so whatever talks to
# MSK directly has to be VPC-attached -- and a VPC-attached Lambda loses
# internet access unless a NAT gateway is added, which is exactly the
# recurring cost this split avoids).
import json
import os

import boto3
import requests

# --- UNCHANGED from get_flight_data.py ---
API_URL = os.getenv("API_URL", "https://opensky-network.org/api/states/all")
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

STATE_FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude",
    "squawk", "spi", "position_source"
]

# --- CHANGED: SQS queue instead of a Kafka broker/topic ---
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
sqs = boto3.client("sqs")


# --- UNCHANGED from get_flight_data.py ---
def get_access_token():
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    response = requests.post(TOKEN_URL, data=data)
    response.raise_for_status()  # throws error if not 200 OK

    return response.json()["access_token"]


# --- UNCHANGED from get_flight_data.py ---
def to_structured_state(raw_state):
    return {STATE_FIELDS[i]: raw_state[i] if i < len(raw_state) else None
            for i in range(len(STATE_FIELDS))}


# --- UNCHANGED from get_flight_data.py ---
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


# --- CHANGED: replaces create_kafka_producer()/producer.send()/producer.flush().
# Sends in batches of up to 10 (SQS's SendMessageBatch limit) instead of
# streaming one-by-one to a persistent Kafka producer connection. ---
def send_to_queue(states):
    sent = 0
    batch = []
    for i, state in enumerate(states, start=1):
        structured = to_structured_state(state)
        batch.append({"Id": str(i), "MessageBody": json.dumps(structured)})
        if len(batch) == 10:
            sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=batch)
            sent += len(batch)
            batch = []
    if batch:
        sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=batch)
        sent += len(batch)
    return sent


# --- CHANGED: Lambda entry point, replacing `if __name__ == "__main__": run()`.
# Same control flow as the original run(), just SQS instead of Kafka as the sink. ---
def lambda_handler(event, context):
    data = fetch_data_from_api()

    if not data or 'states' not in data:
        print("No data fetched this run.")
        return {"sent": 0}

    states = data['states']
    if isinstance(states, list):
        sent = send_to_queue(states)
    else:
        sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(states))
        sent = 1

    print(f"Sent {sent} messages to {SQS_QUEUE_URL}")
    return {"sent": sent}
