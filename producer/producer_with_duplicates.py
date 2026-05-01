import json
import time
import logging

from kafka import KafkaProducer
from datetime import datetime
from common.config import KAFKA_BROKER, TOPIC_SALES


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
    # Kafka Producer Setup
    # ==================================
    # The producer connects to the configured Kafka broker
    # and publishes events to the sales topic.
    #
    # value_serializer converts Python dictionaries
    # into JSON-encoded bytes before sending.
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # ==================================
    # Test Events (Including Duplicates)
    # ==================================
    # The following dataset intentionally contains
    # repeated order_id values.
    #
    # These duplicates should be detected and skipped
    # by the consumer's Redis-based deduplication logic.
    events = [
        {
            "event_id": "DUP-1001-A",
            "order_id": "CA-DUP-1001",
            "region": "East",
            "category": "Technology",
            "sub_category": "Phones",
            "sales": 250.0,
            "profit": 25.0,
            "discount": 0.1,
            "event_time": datetime.utcnow().isoformat() + "Z",
        },
        {
            "event_id": "DUP-1002-A",
            "order_id": "CA-DUP-1002",
            "region": "West",
            "category": "Technology",
            "sub_category": "Phones",
            "sales": 420.0,
            "profit": 42.0,
            "discount": 0.1,
            "event_time": datetime.utcnow().isoformat() + "Z",
        },
        {
            "event_id": "DUP-1001-B",
            "order_id": "CA-DUP-1001",
            "region": "East",
            "category": "Technology",
            "sub_category": "Phones",
            "sales": 250.0,
            "profit": 25.0,
            "discount": 0.1,
            "event_time": datetime.utcnow().isoformat() + "Z",
        },
    ]

    # ==================================
    # Event Publishing Loop
    # ==================================
    # Publish each event to Kafka sequentially.
    #
    # A short delay is added between events
    # to make consumer logs easier to read.
    for event in events:

        producer.send(
            topic=TOPIC_SALES,
            key=event["region"].encode(),
            value=event
        )

        producer.flush()

        logger.info("Duplicate test event sent: %s", event)

        # Small delay for readability in logs
        time.sleep(1)


# ==================================
# Script Entry Point
# ==================================
if __name__ == "__main__":
    main()