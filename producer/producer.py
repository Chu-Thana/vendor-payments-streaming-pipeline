from __future__ import annotations

import json
import logging
import random
import sys
import time
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
    TOPIC_VENDOR_PAYMENTS,
)
from common.event_builder import (  # noqa: E402
    build_stream_window_complete_event,
    build_vendor_payment_event,
)

from common.reporting import (  # noqa: E402
    build_producer_execution_report,
    write_producer_execution_report,
)


logging.basicConfig(
    level=getattr(
        logging,
        LOG_LEVEL.upper(),
        logging.INFO,
    ),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def build_kafka_producer() -> KafkaProducer:
    """Build Kafka producer for local Kafka or cloud Kafka."""
    config: dict[str, Any] = {
        "bootstrap_servers": KAFKA_BROKER,
        "value_serializer": lambda value: json.dumps(
            value
        ).encode("utf-8"),
        "acks": "all",
        "enable_idempotence": True,
        "delivery_timeout_ms": 300000,
        "request_timeout_ms": 60000,
        "max_block_ms": 120000,
        "linger_ms": 5,
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


def _build_message_key(
    event: dict[str, Any],
) -> bytes:
    """Build Kafka message key for partitioning."""
    key = (
        event.get("business_composite_key")
        or event.get("source_row_hash")
        or event["event_id"]
    )

    return str(key).encode("utf-8")


def load_vendor_payment_events(
    source_file: Path,
) -> tuple[
    list[dict[str, Any]],
    int,
]:
    """Load one streaming input window and build base Kafka events."""
    if not source_file.exists():
        raise FileNotFoundError(
            f"Streaming input file not found: "
            f"{source_file}"
        )

    dataframe = pd.read_csv(
        source_file,
        dtype={
            "purchase_order": "string",
        },
    )

    if dataframe.empty:
        raise ValueError(
            f"Streaming input file is empty: "
            f"{source_file}"
        )

    source_row_count = len(dataframe)

    events = []

    for _, row in dataframe.iterrows():
        event = build_vendor_payment_event(row)
        events.append(event)

    return events, source_row_count


def inject_duplicate_events(
    events: list[dict[str, Any]],
    duplicate_rate: float = DUPLICATE_RATE,
    random_seed: int = RANDOM_SEED,
) -> list[dict[str, Any]]:
    """Inject duplicate events by reusing event IDs and payloads."""
    if not events:
        return []

    duplicate_count = int(
        len(events) * duplicate_rate
    )

    if duplicate_count <= 0:
        return events

    random.seed(random_seed)

    duplicate_events = random.sample(
        events,
        k=min(
            duplicate_count,
            len(events),
        ),
    )

    produced_events = [
        *events,
        *duplicate_events,
    ]

    random.shuffle(produced_events)

    return produced_events


def produce_events(
    events: list[dict[str, Any]],
    acknowledgement_timeout_seconds: int = 30,
    acknowledgement_batch_size: int = 1000,
) -> dict[str, int]:
    """Send events to Kafka and collect acknowledgements in bounded batches."""
    producer = build_kafka_producer()

    metrics = {
        "events_attempted": len(events),
        "events_acknowledged": 0,
        "failed_events": 0,
    }

    pending_deliveries: list[tuple[str, Any]] = []

    logger.info(
        (
            "Producer started | broker=%s | "
            "security_protocol=%s | topic=%s"
        ),
        KAFKA_BROKER,
        KAFKA_SECURITY_PROTOCOL,
        TOPIC_VENDOR_PAYMENTS,
    )

    def resolve_pending_deliveries() -> None:
        for event_id, delivery_future in pending_deliveries:
            try:
                delivery_future.get(
                    timeout=acknowledgement_timeout_seconds
                )

                metrics["events_acknowledged"] += 1

                if (
                    metrics["events_acknowledged"] % 10000
                    == 0
                ):
                    logger.info(
                        "Producer progress | "
                        "acknowledged=%s/%s",
                        f"{metrics['events_acknowledged']:,}",
                        f"{metrics['events_attempted']:,}",
                    )

            except Exception as error:
                metrics["failed_events"] += 1

                logger.error(
                    (
                        "Kafka delivery acknowledgement failed | "
                        "event_id=%s error=%s"
                    ),
                    event_id,
                    str(error),
                )

        pending_deliveries.clear()

    try:
        for event in events:
            try:
                delivery_future = producer.send(
                    topic=TOPIC_VENDOR_PAYMENTS,
                    key=_build_message_key(event),
                    value=event,
                )

                pending_deliveries.append(
                    (
                        str(event["event_id"]),
                        delivery_future,
                    )
                )

                if (
                    len(pending_deliveries)
                    >= acknowledgement_batch_size
                ):
                    resolve_pending_deliveries()

            except Exception as error:
                metrics["failed_events"] += 1

                logger.error(
                    (
                        "Kafka send request failed | "
                        "event_id=%s error=%s"
                    ),
                    event.get("event_id"),
                    str(error),
                )

        if pending_deliveries:
            resolve_pending_deliveries()

        producer.flush()

    finally:
        producer.close()

    return metrics


def main(
    source_file: Path,
) -> None:
    """Produce one streaming window and write execution metadata."""

    execution_started_at = time.perf_counter()

    base_events, source_row_count = (
        load_vendor_payment_events(
            source_file
        )
    )

    window_ids = {
        str(event["window_id"])
        for event in base_events
    }

    if len(window_ids) != 1:
        raise ValueError(
            "Producer execution must contain exactly one window."
        )

    window_id = next(iter(window_ids))

    produced_events = inject_duplicate_events(
        base_events
    )

    delivery_metrics = produce_events(
        produced_events
    )

    if (
        delivery_metrics["failed_events"] == 0
        and delivery_metrics["events_acknowledged"]
        == delivery_metrics["events_attempted"]
    ):
        window_complete_event = (
            build_stream_window_complete_event(
                window_id=window_id,
                expected_event_count=delivery_metrics[
                    "events_acknowledged"
                ],
            )
        )

        window_completion_metrics = produce_events(
            [window_complete_event]
        )

        if (
            window_completion_metrics["failed_events"] > 0
            or window_completion_metrics[
                "events_acknowledged"
            ]
            != 1
        ):
            raise RuntimeError(
                "Failed to publish completion marker "
                f"for window: {window_id}"
            )

    runtime_seconds = (
        time.perf_counter()
        - execution_started_at
    )

    duplicate_events_injected = (
        len(produced_events)
        - len(base_events)
    )

    report = build_producer_execution_report(
        source_file=source_file,
        source_row_count=source_row_count,
        base_event_count=len(base_events),
        duplicate_events_injected=(
            duplicate_events_injected
        ),
        events_attempted=delivery_metrics[
            "events_attempted"
        ],
        events_acknowledged=delivery_metrics[
            "events_acknowledged"
        ],
        failed_events=delivery_metrics[
            "failed_events"
        ],
        runtime_seconds=runtime_seconds,
        duplicate_rate_configured=DUPLICATE_RATE,
        topic=TOPIC_VENDOR_PAYMENTS,
    )

    write_producer_execution_report(report)

    logger.info(
        "Vendor payment streaming production completed."
    )
    logger.info(
        "Producer runtime: %.3f seconds",
        runtime_seconds,
    )
    logger.info(
        "Source rows: %s",
        f"{source_row_count:,}",
    )
    logger.info(
        "Base events: %s",
        f"{len(base_events):,}",
    )
    logger.info(
        "Duplicate events injected: %s",
        f"{duplicate_events_injected:,}",
    )
    logger.info(
        "Events attempted: %s",
        f"{delivery_metrics['events_attempted']:,}",
    )
    logger.info(
        "Events acknowledged: %s",
        f"{delivery_metrics['events_acknowledged']:,}",
    )
    logger.info(
        "Failed events: %s",
        f"{delivery_metrics['failed_events']:,}",
    )
    logger.info(
        "Duplicate rate configured: %.4f",
        DUPLICATE_RATE,
    )
    logger.info(
        "Execution status: %s",
        report["status"],
    )
    logger.info(
        "Validation status: %s",
        report["validation"]["status"],
    )

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: python producer.py <source_file>"
        )

    main(Path(sys.argv[1]))
