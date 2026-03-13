import json
import logging
from pathlib import Path
from datetime import datetime, UTC

import redis
from kafka import KafkaConsumer

from common.config import (
    KAFKA_BROKER,
    TOPIC_SALES,
    HIGH_VALUE_THRESHOLD,
    DEDUP_TTL_SECONDS,
    REDIS_HOST,
    REDIS_PORT,
)


# ==================================
# Logging Setup
# ==================================
# Configure application logging.
#
# This keeps the log format simple and readable while still allowing
# structured fields to be passed through logger.extra.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ==================================
# Consumer Main Function
# ==================================
def main(consumer_name: str) -> None:
    """
    Start a Kafka consumer instance for processing sales events.

    Responsibilities:
    - consume events from Kafka
    - deduplicate events using Redis
    - write processed events to local sinks
    - maintain simple aggregations
    - detect high-value sales alerts
    - persist local and global metrics
    """

    # ==================================
    # Kafka Consumer Setup
    # ==================================
    # Connect to the Kafka broker and subscribe to the sales topic.
    #
    # Configuration notes:
    # - auto_offset_reset="earliest"
    #   Start from the earliest offset if no committed offset exists.
    #
    # - enable_auto_commit=True
    #   Consumer commits offsets automatically.
    #
    # - group_id="sales-consumer-group"
    #   Multiple consumers in the same group share partitions.
    consumer = KafkaConsumer(
        TOPIC_SALES,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="sales-consumer-group"
    )

    # ==================================
    # Redis Client (Deduplication + Global Metrics)
    # ==================================
    # Redis is used for two purposes:
    #
    # 1. Deduplication
    #    Track processed order_id values to prevent duplicate processing.
    #
    # 2. Global metrics
    #    Maintain counters shared across all consumer instances.
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )

    # ==================================
    # Output Directories
    # ==================================
    # Ensure runtime directories exist.
    # These folders act as local sinks and are ignored by git.
    event_dir = Path("output/event")
    event_dir.mkdir(parents=True, exist_ok=True)

    aggregates_dir = Path("output/aggregates")
    aggregates_dir.mkdir(parents=True, exist_ok=True)

    metric_dir = Path("output/metric")
    metric_dir.mkdir(parents=True, exist_ok=True)

    alerts_dir = Path("alerts")
    alerts_dir.mkdir(parents=True, exist_ok=True)

    # ==================================
    # Output Files
    # ==================================
    # Event sink
    output_file = event_dir / "sales_events.jsonl"

    # Aggregation output
    aggregate_file = aggregates_dir / "sales_by_region.jsonl"

    # Consumer-level metrics
    metrics_file = metric_dir / "metrics.json"

    # Global metrics across all consumers
    global_metrics_file = metric_dir / "global_metrics.json"

    # Alert sinks
    alert_file = alerts_dir / "high_value_sales.jsonl"
    duplicate_file = alerts_dir / "duplicate_events.jsonl"

    # ==================================
    # In-Memory Aggregation State
    # ==================================
    # Maintain a running total of sales by region.
    # This state lives in memory for the lifetime of the consumer process.
    sales_by_region = {}

    # ==================================
    # Local Consumer Metrics
    # ==================================
    metrics = {
        "consumer": consumer_name,
        "events_processed": 0,
        "duplicates_skipped": 0,
        "alerts_triggered": 0,
        "last_updated": None
    }

    # ==================================
    # Metrics Utilities
    # ==================================
    def write_metrics() -> None:
        """
        Persist consumer-local metrics to disk.

        This provides a quick view of the current consumer's progress.
        """
        metrics["last_updated"] = datetime.now(UTC).isoformat()

        with metrics_file.open("w", encoding="utf-8") as fo:
            json.dump(metrics, fo, ensure_ascii=False, indent=2)

    def write_global_metrics() -> None:
        """
        Persist aggregated metrics across all consumers.

        Values are retrieved from Redis counters shared by the consumer group.
        """
        global_metrics = {
            "events_processed_total": int(redis_client.get("metrics:events_processed_total") or 0),
            "duplicates_skipped_total": int(redis_client.get("metrics:duplicates_skipped_total") or 0),
            "alerts_triggered_total": int(redis_client.get("metrics:alerts_triggered_total") or 0),
            "last_updated": datetime.now(UTC).isoformat()
        }

        with global_metrics_file.open("w", encoding="utf-8") as fx:
            json.dump(global_metrics, fx, ensure_ascii=False, indent=2)

    # ==================================
    # Startup Logging
    # ==================================
    # Log startup details so it is easy to verify which consumer is running
    # and where each sink file is being written.
    logger.info("%s started and waiting for messages...", consumer_name)
    logger.info("%s writing events to %s", consumer_name, output_file)
    logger.info("%s writing aggregates to %s", consumer_name, aggregate_file)
    logger.info("%s writing metrics to %s", consumer_name, metrics_file)
    logger.info("%s writing global metrics to %s", consumer_name, global_metrics_file)
    logger.info("%s writing alerts to %s", consumer_name, alert_file)
    logger.info("%s writing duplicate skips to %s", consumer_name, duplicate_file)

    # ==================================
    # Main Consumer Loop
    # ==================================
    # Continuously process events from Kafka.
    for message in consumer:

        event = message.value
        partition = message.partition
        offset = message.offset
        topic = message.topic
        order_id = str(event["order_id"])
        region = event["region"]
        sales = event["sales"]

        # ==================================
        # Deduplication (Redis SETNX pattern)
        # ==================================
        # Redis key format:
        #   processed_order:<order_id>
        #
        # SETNX behavior through set(..., nx=True):
        #   - If key does NOT exist -> insert and return True
        #   - If key exists -> return None / False-like value
        #
        # TTL is applied to prevent unbounded Redis key growth.
        redis_key = f"processed_order:{order_id}"
        was_set = redis_client.set(
            redis_key,
            "1",
            nx=True,
            ex=DEDUP_TTL_SECONDS
        )

        if not was_set:
            metrics["duplicates_skipped"] += 1
            redis_client.incr("metrics:duplicates_skipped_total")

            write_metrics()
            write_global_metrics()

            duplicate_event = {
                "detected_at": datetime.now(UTC).isoformat(),
                "consumer": consumer_name,
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "order_id": event["order_id"],
                "region": region,
                "sales": sales,
                "reason": "duplicate_order_id_skipped_redis"
            }

            # Structured duplicate log
            # Keep the message short, and put important fields into extra.
            logger.warning(
                "duplicate skipped",
                extra={
                    "event_type": "duplicate_skipped",
                    "consumer_name": consumer_name,
                    "store": "redis",
                    "order_id": order_id,
                    "topic_name": topic,
                    "partition_id": partition,
                    "offset_value": offset,
                    "region_name": region,
                    "sales_value": sales,
                }
            )

            with duplicate_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(duplicate_event, ensure_ascii=False) + "\n")

            continue

        # ==================================
        # Event Processing
        # ==================================
        # Process only events that pass deduplication.
        metrics["events_processed"] += 1
        redis_client.incr("metrics:events_processed_total")

        enriched_event = {
            "consumer": consumer_name,
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "order_id": event["order_id"],
            "region": region,
            "sales": sales,
        }

        # Structured processed-event log
        # Log important routing and business fields in a stable format.
        logger.info(
            "event processed",
            extra={
                "event_type": "processed",
                "consumer_name": consumer_name,
                "topic_name": topic,
                "partition_id": partition,
                "offset_value": offset,
                "order_id": order_id,
                "region_name": region,
                "sales_value": sales,
            }
        )

        with output_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(enriched_event, ensure_ascii=False) + "\n")

        # ==================================
        # Aggregation (Sales by Region)
        # ==================================
        # Maintain an in-memory running total by region.
        if region not in sales_by_region:
            sales_by_region[region] = {
                "total_sales": 0.0,
                "events_processed": 0
            }

        sales_by_region[region]["total_sales"] += sales
        sales_by_region[region]["events_processed"] += 1

        aggregate_snapshot = {
            "updated_at": datetime.now(UTC).isoformat(),
            "consumer": consumer_name,
            "region": region,
            "total_sales": round(sales_by_region[region]["total_sales"], 2),
            "events_processed": sales_by_region[region]["events_processed"],
        }

        with aggregate_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(aggregate_snapshot, ensure_ascii=False) + "\n")

        # ==================================
        # Alert Detection (High Value Sales)
        # ==================================
        # Trigger an alert when sales exceed the configured threshold.
        if sales > HIGH_VALUE_THRESHOLD:
            metrics["alerts_triggered"] += 1
            redis_client.incr("metrics:alerts_triggered_total")

            alert_event = {
                "alert_type": "high_value_sale",
                "detected_at": datetime.now(UTC).isoformat(),
                "consumer": consumer_name,
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "order_id": event["order_id"],
                "region": region,
                "sales": sales,
            }

            # Structured alert log
            # This keeps the alert line compact and easy to scan.
            logger.warning(
                "high value sale detected",
                extra={
                    "event_type": "high_value_sale",
                    "consumer_name": consumer_name,
                    "order_id": order_id,
                    "topic_name": topic,
                    "partition_id": partition,
                    "offset_value": offset,
                    "region_name": region,
                    "sales_value": sales,
                }
            )

            with alert_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(alert_event, ensure_ascii=False) + "\n")

        # ==================================
        # Persist Metrics
        # ==================================
        # Persist local and global metric snapshots after each processed event.
        write_metrics()
        write_global_metrics()


# ==================================
# Script Entry Point
# ==================================
if __name__ == "__main__":
    import sys

    name = sys.argv[1] if len(sys.argv) > 1 else "consumer-A"
    main(name)