from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from common.alert_notifier import send_telegram_alert  # noqa: E402

from common.config import (  # noqa: E402
    KAFKA_BROKER,
    KAFKA_PASSWORD,
    KAFKA_SASL_MECHANISM,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_USERNAME,
    LOG_LEVEL,
    STAGING_FILE,
    TOPIC_VENDOR_PAYMENTS,
    TELEGRAM_LARGE_PAYMENT_ALERT_LIMIT,
)

from common.large_payment_alert import (  # noqa: E402
    build_large_payment_alert_message,
    is_large_payment_event,
)

from common.dedup import RedisDeduplicator  # noqa: E402
from common.reporting import (  # noqa: E402
    build_streaming_summary_report,
    write_streaming_summary_report,
)
from common.writer import write_event_to_staging  # noqa: E402


logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def build_kafka_consumer(consumer_group: str) -> KafkaConsumer:
    """Build Kafka consumer for local Kafka or cloud Kafka."""
    config: dict[str, Any] = {
        "bootstrap_servers": KAFKA_BROKER,
        "value_deserializer": lambda message: json.loads(message.decode("utf-8")),
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "group_id": consumer_group,
        "consumer_timeout_ms": 10000,
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

    return KafkaConsumer(TOPIC_VENDOR_PAYMENTS, **config)


def connect_consumer_with_retry(
    consumer_group: str,
    max_attempts: int = 5,
    sleep_seconds: int = 3,
) -> KafkaConsumer:
    """Connect to Kafka with simple retry handling."""
    for attempt in range(1, max_attempts + 1):
        try:
            consumer = build_kafka_consumer(consumer_group=consumer_group)
            logger.info(
                "Connected to Kafka | broker=%s | security_protocol=%s | topic=%s | group=%s",
                KAFKA_BROKER,
                KAFKA_SECURITY_PROTOCOL,
                TOPIC_VENDOR_PAYMENTS,
                consumer_group,
            )
            return consumer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not ready, retrying... attempt=%s/%s",
                attempt,
                max_attempts,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError("Kafka broker is still not available after retries")


def validate_event(event: dict[str, Any]) -> None:
    """Validate required fields for a vendor payment streaming event."""
    required_fields = [
        "event_id",
        "event_type",
        "event_timestamp",
        "source_system",
    ]

    missing_fields = [
        field
        for field in required_fields
        if field not in event or event[field] in (None, "")
    ]

    if missing_fields:
        raise ValueError(f"Missing required event fields: {missing_fields}")


def consume_vendor_payment_events(
    consumer_name: str = "consumer-A",
    consumer_group: str = "vendor-payments-consumer-group",
) -> dict[str, int]:
    """Consume vendor payment events, apply Redis dedup, and write staging output."""
    consumer = connect_consumer_with_retry(consumer_group=consumer_group)
    deduplicator = RedisDeduplicator()

    STAGING_FILE.parent.mkdir(parents=True, exist_ok=True)

    metrics = {
        "consumed_events": 0,
        "accepted_events": 0,
        "rejected_duplicates": 0,
        "failed_events": 0,
        "large_payment_alerts_sent": 0,
    }

    logger.info("%s started and waiting for messages...", consumer_name)
    logger.info("%s writing accepted events to %s", consumer_name, STAGING_FILE)

    try:
        for message in consumer:
            event = message.value
            metrics["consumed_events"] += 1

            try:
                validate_event(event)

                event_id = str(event["event_id"])

                if deduplicator.is_duplicate(event_id):
                    metrics["rejected_duplicates"] += 1

                    logger.warning(
                        "duplicate event rejected | consumer=%s topic=%s partition=%s offset=%s event_id=%s",
                        consumer_name,
                        message.topic,
                        message.partition,
                        message.offset,
                        event["event_id"],
                    )

                    consumer.commit()
                    continue

                write_event_to_staging(event)
                metrics["accepted_events"] += 1

                if (
                        is_large_payment_event(event)
                        and metrics["large_payment_alerts_sent"] < TELEGRAM_LARGE_PAYMENT_ALERT_LIMIT
                ):
                    alert_message = build_large_payment_alert_message(event)
                    alert_sent = send_telegram_alert(alert_message)

                    if alert_sent:
                        metrics["large_payment_alerts_sent"] += 1

                    logger.warning(
                        "large payment alert evaluated | consumer=%s event_id=%s alert_sent=%s",
                        consumer_name,
                        event["event_id"],
                        alert_sent,
                    )

                deduplicator.mark_processed(event)

                logger.info(
                    "accepted event | consumer=%s topic=%s partition=%s offset=%s event_id=%s",
                    consumer_name,
                    message.topic,
                    message.partition,
                    message.offset,
                    event["event_id"],
                )

                consumer.commit()

            except Exception as error:
                metrics["failed_events"] += 1

                logger.exception(
                    "event processing failed | consumer=%s topic=%s partition=%s offset=%s error=%s",
                    consumer_name,
                    getattr(message, "topic", None),
                    getattr(message, "partition", None),
                    getattr(message, "offset", None),
                    str(error),
                )

                continue

    finally:
        consumer.close()

    report = build_streaming_summary_report(
        base_events=metrics["accepted_events"],
        produced_events=metrics["consumed_events"],
        accepted_events=metrics["accepted_events"],
        rejected_duplicates=metrics["rejected_duplicates"],
        topic=TOPIC_VENDOR_PAYMENTS,
        staging_file=STAGING_FILE,
    )

    report["large_payment_alerts_sent"] = metrics["large_payment_alerts_sent"]

    if metrics["failed_events"]:
        report["failed_events"] = metrics["failed_events"]

    write_streaming_summary_report(report)

    logger.info("Vendor payment streaming consumption completed.")
    logger.info("Consumed events: %s", f"{metrics['consumed_events']:,}")
    logger.info("Accepted events: %s", f"{metrics['accepted_events']:,}")
    logger.info("Rejected duplicates: %s", f"{metrics['rejected_duplicates']:,}")
    logger.info("Failed events: %s", f"{metrics['failed_events']:,}")

    return metrics


def main(consumer_name: str = "consumer-A") -> None:
    """Run the vendor payments Kafka consumer."""
    consume_vendor_payment_events(consumer_name=consumer_name)


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "consumer-A"
    main(name)