from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:

    event = {
        "order_id": random.randint(1000, 9999),
        "region": random.choice(["East", "West", "Central", "South"]),
        "sales": round(random.uniform(10, 500), 2)
    }

    producer.send(
        "sales_events",
        key=event["region"].encode(),
        value=event
    )

    producer.flush()

    print("Sent:", event)

    time.sleep(2)