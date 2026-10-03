from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer
from kafka.errors import KafkaConnectionError

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
    STAGING_DIR,
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


def build_window_complete_metadata_file(
    window_id: str,
) -> Path:
    return (
        STAGING_DIR
        / window_id
        / "_WINDOW_COMPLETE.json"
    )


def persist_window_expected_count(
    window_id: str,
    expected_event_count: int,
) -> None:
    metadata_file = (
        build_window_complete_metadata_file(window_id)
    )

    metadata_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    payload = {
        "window_id": window_id,
        "expected_event_count": expected_event_count,
    }

    metadata_file.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )


def finalize_known_windows(
    window_expected_counts: dict[str, int],
) -> None:
    for window_dir in STAGING_DIR.glob(
        "stream_window_*"
    ):
        metadata_file = (
            window_dir
            / "_WINDOW_COMPLETE.json"
        )

        if not metadata_file.exists():
            continue

        try_finalize_window(
            window_id=window_dir.name,
            window_expected_counts=window_expected_counts,
        )


def build_staging_file(
    window_id: str,
) -> Path:
    return (
        STAGING_DIR
        / window_id
        / "events.jsonl"
    )


def is_window_complete_event(
    event: dict[str, Any],
) -> bool:
    return (
        event.get("event_type")
        == "stream_window_complete"
    )


def build_success_marker(
    window_id: str,
) -> Path:
    return (
        STAGING_DIR
        / window_id
        / "_SUCCESS"
    )


def try_finalize_window(
    window_id: str,
    window_expected_counts: dict[str, int],
) -> None:
    expected_count = window_expected_counts.get(
        window_id
    )

    if expected_count is None:
        metadata_file = (
            build_window_complete_metadata_file(
                window_id
            )
        )

        if not metadata_file.exists():
            return

        metadata = json.loads(
            metadata_file.read_text(
                encoding="utf-8"
            )
        )

        expected_count = int(
            metadata["expected_event_count"]
        )

        window_expected_counts[
            window_id
        ] = expected_count

    staged_count = (
        count_unique_staged_events(
            window_id
        )
    )

    if staged_count != expected_count:
        return

    success_marker = (
        build_success_marker(
            window_id
        )
    )

    success_marker.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    success_marker.touch()


def count_unique_staged_events(
    window_id: str,
) -> int:
    """Count unique event IDs durably written for one window."""

    staging_file = build_staging_file(
        window_id
    )

    if not staging_file.exists():
        return 0

    event_ids: set[str] = set()

    with staging_file.open(
        "r",
        encoding="utf-8",
    ) as file:
        for line in file:
            if not line.strip():
                continue

            event = json.loads(line)

            event_id = event.get(
                "event_id"
            )

            if event_id:
                event_ids.add(
                    str(event_id)
                )

    return len(event_ids)


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
        except KafkaConnectionError:
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
        "window_id",
    ]

    missing_fields = []

    for field in required_fields:
        if field not in event or event[field] in (None, ""):
            missing_fields.append(field)

    if missing_fields:
        raise ValueError(
            f"Missing required event fields: {missing_fields}"
        )


def consume_vendor_payment_events(
    consumer_name: str = "consumer-A",
    consumer_group: str = "vendor-payments-consumer-group",
) -> dict[str, int]:
    """Consume events, apply Redis deduplication, and write execution metadata."""
    execution_started_at = time.perf_counter()

    consumer = connect_consumer_with_retry(
        consumer_group=consumer_group,
    )
    deduplicator = RedisDeduplicator()

    staging_files: set[Path] = set()

    window_expected_counts: dict[str, int] = {}

    metrics = {
        "consumed_events": 0,
        "accepted_events": 0,
        "rejected_duplicates": 0,
        "failed_events": 0,
        "large_payment_alerts_sent": 0,
    }

    logger.info(
        "%s started and waiting for messages...",
        consumer_name,
    )
    logger.info(
        "%s writing accepted events by window under %s",
        consumer_name,
        STAGING_DIR,
    )

    try:
        for message in consumer:
            event = message.value

            is_control_event = is_window_complete_event(
                event
            )

            if not is_control_event:
                metrics["consumed_events"] += 1

            try:
                validate_event(event)

                window_id = str(event["window_id"])

                if is_control_event:
                    expected_event_count = int(
                        event["expected_event_count"]
                    )

                    window_expected_counts[
                        window_id
                    ] = expected_event_count

                    persist_window_expected_count(
                        window_id=window_id,
                        expected_event_count=expected_event_count,
                    )

                    try_finalize_window(
                        window_id=window_id,
                        window_expected_counts=window_expected_counts,
                    )

                    consumer.commit()
                    continue

                # From here down = normal business event
                event_id = str(event["event_id"])

                staging_file = build_staging_file(
                    window_id
                )

                # Duplicate handling
                if deduplicator.is_duplicate(event_id):
                    metrics["rejected_duplicates"] += 1
                    consumer.commit()
                    continue

                for attempt in range(1, 4):
                    try:
                        write_event_to_staging(
                            event,
                            staging_file,
                        )
                        break

                    except Exception as error:
                        if attempt == 3:
                            raise

                        logger.warning(
                            (
                                "staging write failed, retrying | "
                                "event_id=%s attempt=%s error=%s"
                            ),
                            event_id,
                            attempt,
                            str(error),
                        )

                        time.sleep(2)

                staging_files.add(staging_file)
                metrics["accepted_events"] += 1

                if (
                    is_large_payment_event(event)
                    and metrics["large_payment_alerts_sent"]
                    < TELEGRAM_LARGE_PAYMENT_ALERT_LIMIT
                ):
                    alert_message = build_large_payment_alert_message(
                        event
                    )
                    alert_sent = send_telegram_alert(
                        alert_message
                    )

                    if alert_sent:
                        metrics["large_payment_alerts_sent"] += 1

                    logger.warning(
                        (
                            "large payment alert evaluated | "
                            "consumer=%s event_id=%s "
                            "alert_sent=%s"
                        ),
                        consumer_name,
                        event_id,
                        alert_sent,
                    )

                deduplicator.mark_processed(event)

                logger.info(
                    (
                        "accepted event | consumer=%s topic=%s "
                        "partition=%s offset=%s event_id=%s"
                    ),
                    consumer_name,
                    message.topic,
                    message.partition,
                    message.offset,
                    event_id,
                )

                consumer.commit()

            except Exception as error:
                metrics["failed_events"] += 1
                logger.exception(
                    (
                        "event processing failed | consumer=%s "
                        "topic=%s partition=%s offset=%s error=%s"
                    ),
                    consumer_name,
                    getattr(message, "topic", None),
                    getattr(message, "partition", None),
                    getattr(message, "offset", None),
                    str(error),
                )
                raise

        finalize_known_windows(
            window_expected_counts
        )

    finally:
        consumer.close()

    runtime_seconds = (
        time.perf_counter()
        - execution_started_at
    )

    report = build_streaming_summary_report(
        consumed_events=metrics["consumed_events"],
        accepted_events=metrics["accepted_events"],
        rejected_duplicates=metrics["rejected_duplicates"],
        failed_events=metrics["failed_events"],
        large_payment_alerts_sent=(
            metrics["large_payment_alerts_sent"]
        ),
        runtime_seconds=runtime_seconds,
        consumer_group=consumer_group,
        topic=TOPIC_VENDOR_PAYMENTS,
        staging_files=staging_files,
    )

    write_streaming_summary_report(report)

    logger.info(
        "Vendor payment streaming consumption completed."
    )
    logger.info(
        "Consumer runtime: %.3f seconds",
        runtime_seconds,
    )
    logger.info(
        "Consumed events: %s",
        f"{metrics['consumed_events']:,}",
    )
    logger.info(
        "Accepted events: %s",
        f"{metrics['accepted_events']:,}",
    )
    logger.info(
        "Rejected duplicates: %s",
        f"{metrics['rejected_duplicates']:,}",
    )
    logger.info(
        "Failed events: %s",
        f"{metrics['failed_events']:,}",
    )
    logger.info(
        "Large-payment alerts sent: %s",
        f"{metrics['large_payment_alerts_sent']:,}",
    )
    logger.info(
        "Execution status: %s",
        report["status"],
    )
    logger.info(
        "Validation status: %s",
        report["validation"]["status"],
    )

    return metrics


def main(consumer_name: str = "consumer-A") -> None:
    """Run the vendor payments Kafka consumer."""
    consume_vendor_payment_events(consumer_name=consumer_name)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        name = sys.argv[1]
    else:
        name = "consumer-A"
    main(name)