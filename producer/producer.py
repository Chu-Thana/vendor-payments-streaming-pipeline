import json
import time
import random
import logging
from datetime import datetime

from kafka import KafkaProducer


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
    Start the standard Kafka producer.

    This producer continuously generates synthetic sales events
    and publishes them to the configured Kafka topic.
    """

    # ==================================
    # Kafka Producer Setup
    # ==================================
    # The producer connects to the Kafka broker and publishes
    # simulated sales events to the configured topic.
    #
    # value_serializer ensures events are encoded as JSON bytes.
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # ==================================
    # Event Generation Loop
    # ==================================
    # Continuously generate synthetic sales events.
    #
    # These events simulate real-time incoming transactions
    # from an upstream business system.
    while True:

        # ==================================
        # Simulated Sales Event
        # ==================================
        if random.random() < 0.2:
            event = {
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
                "order_id": f"CA-2011-{random.randint(1000, 9999)}",
                "region": random.choice(["East", "West", "Central", "South"]),
                "category": random.choice(["Technology", "Furniture", "Office Supplies"]),
                "sub_category": random.choice(["Phones", "Chairs", "Binders", "Storage"]),
                "sales": round(random.uniform(50, 5000), 2),
                "profit": round(random.uniform(50, 1500), 2),
                "discount": round(random.choice([0.0, 0.1, 0.2, 0.3]), 2),
                "event_time": datetime.utcnow().isoformat() + "Z"
            }

        # ==================================
        # Kafka Publish
        # ==================================
        # The region is used as the partition key.
        #
        # This helps route events with the same region
        # to the same partition, preserving ordering
        # within that key group.
        producer.send(
            topic=TOPIC_SALES,
            key=event["region"].encode(),
            value=event
        )

        producer.flush()

        logger.info("Event produced: %s", event)

        # ==================================
        # Throttle Event Rate
        # ==================================
        # Pause briefly to simulate a steady event stream.
        time.sleep(2)


# ==================================
# Script Entry Point
# ==================================
if __name__ == "__main__":
    main()