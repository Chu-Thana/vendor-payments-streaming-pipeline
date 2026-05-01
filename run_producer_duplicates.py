import json
import time
import logging
from datetime import datetime, UTC

from kafka import KafkaProducer

from common.config import (
    KAFKA_BROKER,
    TOPIC_SALES,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM,
    KAFKA_USERNAME,
    KAFKA_PASSWORD,
)

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

producer = KafkaProducer(**config)


# ==================================
# Logging Setup
# ==================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ==================================
# Producer Main Function
# ==================================
def main() -> None:
    """
    Start a Kafka producer that publishes test events with duplicates.

    This producer is designed specifically to validate the deduplication
    mechanism implemented in the consumer.

    The dataset includes intentionally repeated order_id values.
    The consumer should detect and skip these duplicates using Redis.
    """

    # ==================================
    # Test Events (Including Duplicates)
    # ==================================
    # Use the same event schema as the main producer so the consumer
    # can process these test events without schema mismatch.
    events = [
        {
            "event_id": "EV-DUP-1001",
            "order_id": "CA-2011-1001",
            "region": "East",
            "category": "Technology",
            "sub_category": "Phones",
            "sales": 250.0,
            "profit": 25.0,
            "discount": 0.1,
            "event_time": datetime.now(UTC).isoformat()
        },
        {
            "event_id": "EV-DUP-1002",
            "order_id": "CA-2011-1002",
            "region": "West",
            "category": "Furniture",
            "sub_category": "Chairs",
            "sales": 420.0,
            "profit": 60.0,
            "discount": 0.2,
            "event_time": datetime.now(UTC).isoformat()
        },
        {
            "event_id": "EV-DUP-1001",  # duplicate event_id
            "order_id": "CA-2011-1001",
            "region": "East",
            "category": "Technology",
            "sub_category": "Phones",
            "sales": 250.0,
            "profit": 25.0,
            "discount": 0.1,
            "event_time": datetime.now(UTC).isoformat()
        },
        {
            "event_id": "EV-DUP-1003",
            "order_id": "CA-2011-1003",
            "region": "Central",
            "category": "Office Supplies",
            "sub_category": "Binders",
            "sales": 180.0,
            "profit": 15.0,
            "discount": 0.0,
            "event_time": datetime.now(UTC).isoformat()
        },
        {
            "event_id": "EV-DUP-1002",  # duplicate event_id
            "order_id": "CA-2011-1002",
            "region": "West",
            "category": "Furniture",
            "sub_category": "Chairs",
            "sales": 420.0,
            "profit": 60.0,
            "discount": 0.2,
            "event_time": datetime.now(UTC).isoformat()
        },
        {
            "event_id": "EV-DUP-1004",
            "order_id": "CA-2011-1004",
            "region": "South",
            "category": "Technology",
            "sub_category": "Phones",
            "sales": 510.0,
            "profit": 80.0,
            "discount": 0.3,
            "event_time": datetime.now(UTC).isoformat()
        },
    ]

    for _ in range(200):
        for event in events:
            producer.send(
                topic=TOPIC_SALES,
                key=event["region"].encode(),
                value=event
            )

    producer.flush()
    logger.info("Finished sending duplicate test events")


# ==================================
# Script Entry Point
# ==================================
if __name__ == "__main__":
    main()