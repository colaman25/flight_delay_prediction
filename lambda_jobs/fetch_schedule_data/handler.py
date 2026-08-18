# Lambda port of proj_docker/get_schedule_data/get_schedule_data.py.
#
# Fetches FlightAware departures and arrivals and publishes them to SQS
# instead of directly to Kafka -- a separate, VPC-attached Lambda
# (publish_to_kafka) consumes these queues and produces to MSK. This
# Lambda deliberately stays outside the VPC so it keeps free internet
# access to call the FlightAware API (see fetch_flight_data/handler.py
# for the full reasoning -- same split, same reason).
import json
import os
import time

import boto3
import requests
import yaml

# --- UNCHANGED from get_schedule_data.py ---
API_KEY = os.getenv("FLIGHTAWARE_API")
headers = {
    'x-apikey': API_KEY,
}

# --- CHANGED: one SQS queue per direction instead of one Kafka topic per
# direction ---
DEPARTURE_QUEUE_URL = os.environ["DEPARTURE_QUEUE_URL"]
ARRIVAL_QUEUE_URL = os.environ["ARRIVAL_QUEUE_URL"]
sqs = boto3.client("sqs")


# --- UNCHANGED from get_schedule_data.py ---
def load_config(config_path='config.yaml'):
    """Loads configuration from a YAML file."""
    with open(config_path, 'r') as file:
        # Use safe_load for security when working with configuration files
        config = yaml.safe_load(file)
    return config


# --- UNCHANGED from get_schedule_data.py, including the 429 retry/backoff
# logic added earlier this session ---
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


# --- CHANGED: takes a queue_url instead of a kafka_topic/producer, and
# sends via SQS SendMessageBatch (batches of up to 10) instead of
# one-by-one to a Kafka producer. Pagination/retry control flow is
# otherwise identical to the original collect_flights(). ---
def collect_flights(queue_url, airport_icao, max_pages, direction):
    """Fetch paginated flight data for one direction (departures/arrivals) and send it to SQS."""
    part_url = f'/airports/{airport_icao}/flights/{direction}'
    print(f"Collector started. Fetching from https://aeroapi.flightaware.com/aeroapi{part_url}")

    total_sent = 0
    page = 1
    while page <= max_pages:
        data = fetch_data_from_api(part_url)
        if not data:
            print("No data fetched this run.")
            break

        flights = data.get(direction) or data.get(f"scheduled_{direction}")
        if flights:
            batch = []
            for i, flight in enumerate(flights, start=1):
                batch.append({"Id": str(i), "MessageBody": json.dumps(flight)})
                if len(batch) == 10:
                    sqs.send_message_batch(QueueUrl=queue_url, Entries=batch)
                    total_sent += len(batch)
                    batch = []
            if batch:
                sqs.send_message_batch(QueueUrl=queue_url, Entries=batch)
                total_sent += len(batch)

        # Prepare next page if available
        if 'links' in data and 'next' in data['links']:
            part_url = data["links"]["next"]
            page += 1
        else:
            print("Reached last page.")
            break

    print(f"Sent {total_sent} messages to {queue_url}")
    return total_sent


# --- CHANGED: Lambda entry point, replacing `if __name__ == "__main__": run()`.
# The original run()'s Kafka-producer-creation retry loop (waiting for the
# broker to be reachable) is dropped entirely -- SQS calls are plain
# synchronous HTTP requests with no persistent broker connection to wait
# for, so there's nothing to retry-connect to here. ---
def lambda_handler(event, context):
    config = load_config('config.yaml')
    data_cfg = config['data_config']
    airport_icao = data_cfg['schedule_airport']
    max_pages = data_cfg.get('max_pages', 1)

    departures_sent = collect_flights(DEPARTURE_QUEUE_URL, airport_icao, max_pages, "departures")
    arrivals_sent = collect_flights(ARRIVAL_QUEUE_URL, airport_icao, max_pages, "arrivals")

    return {"departures_sent": departures_sent, "arrivals_sent": arrivals_sent}
