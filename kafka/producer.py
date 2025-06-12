import requests
from confluent_kafka import Producer

# === CONFIG ===
KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'gc-batches'
API_URL = 'http://gc-challenger:8866'

# === SETUP ===
p = Producer({'bootstrap.servers': KAFKA_BROKER})
session = requests.Session()

# === CREATE AND START BENCHMARK ===
print("Creating benchmark session...")
create_resp = session.post(f"{API_URL}/api/create", json={
    "apitoken": "polimi-deib",
    "name": "kafka-pipeline",
    "test": True
})
create_resp.raise_for_status()
bench_id = create_resp.json()
print(f"Benchmark ID: {bench_id}")

print("Starting benchmark...")
session.post(f"{API_URL}/api/start/{bench_id}").raise_for_status()

# === PRODUCE BATCHES ===
i = 0
while True:
    print(f"Requesting batch {i}...")
    batch_resp = session.get(f"{API_URL}/api/next_batch/{bench_id}")
    
    if batch_resp.status_code == 404:
        print("No more batches.")
        break

    batch_resp.raise_for_status()

    # batch_resp.content è già in formato msgpack
    p.produce(TOPIC_NAME, value=batch_resp.content)
    p.flush()
    print(f"Produced batch {i} to Kafka.")
    i += 1

# === END BENCHMARK ===
session.post(f"{API_URL}/api/end/{bench_id}")
print("Finished producing batches.")
