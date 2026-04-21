import json
import time
import random
import logging
from datetime import datetime

from kafka import KafkaProducer

from common.config import (
    KAFKA_BROKER,
    TOPIC_SALES,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM,
    KAFKA_USERNAME,
    KAFKA_PASSWORD,
    LOG_LEVEL,
)

# ==================================
# Logging Setup
# ==================================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def build_kafka_producer() -> KafkaProducer:
    config = {
        "bootstrap_servers": KAFKA_BROKER,
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
    }

    if KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        config.update({
            "security_protocol": KAFKA_SECURITY_PROTOCOL,
            "sasl_mechanism": KAFKA_SASL_MECHANISM,
            "sasl_plain_username": KAFKA_USERNAME,
            "sasl_plain_password": KAFKA_PASSWORD,
        })

    return KafkaProducer(**config)


def main() -> None:
    """
    Start the Kafka producer.

    Works for both:
    - local Kafka (PLAINTEXT)
    - cloud Kafka such as Confluent Cloud (SASL_SSL)
    """
    producer = build_kafka_producer()

    logger.info(
        "Producer started | broker=%s | security_protocol=%s | topic=%s",
        KAFKA_BROKER,
        KAFKA_SECURITY_PROTOCOL,
        TOPIC_SALES,
    )

    while True:
        if random.random() < 0.2:
            event = {
                "event_id": f"EV-{int(time.time() * 1000)}-{random.randint(1000, 9999)}",
                "order_id": f"CA-ALERT-{random.randint(1000, 9999)}",
                "region": random.choice(["East", "West", "Central", "South"]),
                "category": "Technology",
                "sub_category": "Phones",
                "sales": round(random.uniform(10000, 20000), 2),
                "profit": round(random.uniform(-200, 200), 2),
                "discount": 0.4,
                "event_time": datetime.utcnow().isoformat() + "Z"
            }
        else:
            event = {
                "event_id": f"EV-{int(time.time() * 1000)}-{random.randint(1000, 9999)}",
                "order_id": f"CA-2011-{random.randint(1000, 9999)}",
                "region": random.choice(["East", "West", "Central", "South"]),
                "category": random.choice(["Technology", "Furniture", "Office Supplies"]),
                "sub_category": random.choice(["Phones", "Chairs", "Binders", "Storage"]),
                "sales": round(random.uniform(50, 5000), 2),
                "profit": round(random.uniform(50, 1500), 2),
                "discount": round(random.choice([0.0, 0.1, 0.2, 0.3]), 2),
                "event_time": datetime.utcnow().isoformat() + "Z"
            }

        producer.send(
            topic=TOPIC_SALES,
            key=(event.get("region") or "unknown").encode(),
            value=event
        )
        producer.flush()

        logger.info("Event produced: %s", event)
        time.sleep(2)


if __name__ == "__main__":
    main()