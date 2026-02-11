import time
from confluent_kafka.admin import AdminClient, NewTopic

# Wait for Kafka to be ready
time.sleep(10)

admin = AdminClient({
    "bootstrap.servers": "kafka:9092"
})

topics = [
    NewTopic("flight-data", 1, 1),
    NewTopic("schedule-data", 1, 1),
    NewTopic("prediction-results", 1, 1),
]

fs = admin.create_topics(topics)

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic '{topic}' created.")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")