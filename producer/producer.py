from __future__ import annotations

import json
import logging
import random
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from kafka import KafkaProducer

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from common.config import (  # noqa: E402
    DUPLICATE_RATE,
    KAFKA_BROKER,
    KAFKA_PASSWORD,
    KAFKA_SASL_MECHANISM,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_USERNAME,
    LOG_LEVEL,
    RANDOM_SEED,
    STREAM_SAMPLE_FILE,
    TOPIC_VENDOR_PAYMENTS,
)
from common.event_builder import build_vendor_payment_event  # noqa: E402


logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def build_kafka_producer() -> KafkaProducer:
    """Build Kafka producer for local Kafka or cloud Kafka."""
    config: dict[str, Any] = {
        "bootstrap_servers": KAFKA_BROKER,
        "value_serializer": lambda value: json.dumps(value).encode("utf-8"),
    }

    if KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        config.update(
            {
                "security_protocol": KAFKA_SECURITY_PROTOCOL,
                "sasl_mechanism": KAFKA_SASL_MECHANISM,
                "sasl_plain_username": KAFKA_USERNAME,
                "sasl_plain_password": KAFKA_PASSWORD,
            }
        )

    return KafkaProducer(**config)


def _build_message_key(event: dict[str, Any]) -> bytes:
    """Build Kafka message key for partitioning."""
    key = (
        event.get("business_composite_key")
        or event.get("source_row_hash")
        or event["event_id"]
    )

    return str(key).encode("utf-8")


def load_vendor_payment_events() -> list[dict[str, Any]]:
    """Load streaming sample CSV and convert rows into Kafka events."""
    if not STREAM_SAMPLE_FILE.exists():
        raise FileNotFoundError(
            f"Streaming sample file not found: {STREAM_SAMPLE_FILE}. "
            "Run scripts/prepare_stream_sample.py first."
        )

    df = pd.read_csv(STREAM_SAMPLE_FILE)

    if df.empty:
        raise ValueError(f"Streaming sample file is empty: {STREAM_SAMPLE_FILE}")

    return [
        build_vendor_payment_event(row)
        for _, row in df.iterrows()
    ]


def inject_duplicate_events(
    events: list[dict[str, Any]],
    duplicate_rate: float = DUPLICATE_RATE,
    random_seed: int = RANDOM_SEED,
) -> list[dict[str, Any]]:
    """Inject duplicate events by reusing the same event_id and payload."""
    if not events:
        return []

    duplicate_count = int(len(events) * duplicate_rate)

    if duplicate_count <= 0:
        return events

    random.seed(random_seed)

    duplicate_events = random.sample(
        events,
        k=min(duplicate_count, len(events)),
    )

    produced_events = [*events, *duplicate_events]
    random.shuffle(produced_events)

    return produced_events


def produce_events(events: list[dict[str, Any]]) -> None:
    """Send vendor payment events to Kafka."""
    producer = build_kafka_producer()

    logger.info(
        "Producer started | broker=%s | security_protocol=%s | topic=%s",
        KAFKA_BROKER,
        KAFKA_SECURITY_PROTOCOL,
        TOPIC_VENDOR_PAYMENTS,
    )

    for event in events:
        producer.send(
            topic=TOPIC_VENDOR_PAYMENTS,
            key=_build_message_key(event),
            value=event,
        )

    producer.flush()
    producer.close()


def main() -> None:
    """Produce vendor payment events with intentional duplicate injection."""
    base_events = load_vendor_payment_events()
    produced_events = inject_duplicate_events(base_events)

    produce_events(produced_events)

    duplicate_events = len(produced_events) - len(base_events)

    logger.info("Vendor payment streaming production completed.")
    logger.info("Base events: %s", f"{len(base_events):,}")
    logger.info("Duplicate events injected: %s", f"{duplicate_events:,}")
    logger.info("Total events produced: %s", f"{len(produced_events):,}")
    logger.info("Duplicate rate configured: %.4f", DUPLICATE_RATE)


if __name__ == "__main__":
    main()