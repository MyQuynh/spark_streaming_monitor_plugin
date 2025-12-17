import json
import random
import time
from kafka import KafkaProducer

# Adjust to your brokers
BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "dummy-topic"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",
    retries=3,
)


def build_dummy_event(i: int) -> dict:
    return {
        "event_id": i,
        "user_id": f"user_{random.randint(1, 1000)}",
        "amount": round(random.uniform(1, 1000), 2),
        "status": random.choice(["NEW", "PROCESSING", "DONE", "FAILED"]),
        "ts": int(time.time() * 1000),
    }


for i in range(1000):
    key = f"user-{random.randint(1, 100)}"
    event = build_dummy_event(i)

    future = producer.send(
        TOPIC,
        key=key,
        value=event,
    )

    # Block to surface errors immediately (optional)
    try:
        record_meta = future.get(timeout=10)
        print(record_meta.topic, record_meta.partition, record_meta.offset)
    except Exception as e:
        print("Send failed:", e)

    time.sleep(0.01)  # throttle a bit

producer.flush()
producer.close()
