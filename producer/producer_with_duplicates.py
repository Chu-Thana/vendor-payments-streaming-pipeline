from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

events = [
    {"order_id": 1001, "region": "East", "sales": 250.0},
    {"order_id": 1002, "region": "West", "sales": 420.0},
    {"order_id": 1001, "region": "East", "sales": 250.0},   # duplicate
    {"order_id": 1003, "region": "Central", "sales": 180.0},
    {"order_id": 1002, "region": "West", "sales": 420.0},   # duplicate
    {"order_id": 1004, "region": "South", "sales": 510.0},
]

for event in events:
    producer.send(
        "sales_events",
        key=event["region"].encode(),
        value=event
    )
    producer.flush()

    print("Sent:", event)
    time.sleep(1)