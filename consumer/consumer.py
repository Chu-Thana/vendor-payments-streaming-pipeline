import json
import logging
from pathlib import Path
from datetime import datetime, UTC, timezone

import time
from kafka.errors import NoBrokersAvailable

import redis
from kafka import KafkaConsumer, KafkaProducer
from common.alert_notifier import send_telegram_alert
from common.config import (
    KAFKA_BROKER,
    TOPIC_SALES,
    TOPIC_ALERTS,
    TOPIC_DUPLICATES,
    HIGH_VALUE_THRESHOLD,
    HIGH_DISCOUNT_THRESHOLD,
    LOW_PROFIT_THRESHOLD,
    DEDUP_TTL_SECONDS,
    REDIS_HOST,
    REDIS_PORT,
    STAGING_FILE
)

# ==================================
# Helper function
# ==================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def append_jsonl(file_path: Path, record: dict) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def build_staging_record(event, is_duplicate, is_high_value, consumer_name):
    return {
        "event_id": event.get("event_id"),
        "order_id": event.get("order_id"),
        "region": event.get("region"),
        "category": event.get("category"),
        "sub_category": event.get("sub_category"),
        "sales": event.get("sales"),
        "profit": event.get("profit"),
        "discount": event.get("discount"),
        "event_time": event.get("event_time"),

        "ingested_at": utc_now(),

        "is_duplicate": is_duplicate,
        "is_high_value": is_high_value,

        "consumer_name": f"consumer-{consumer_name}",
        "pipeline_stage": "staged"
    }

# ==================================
# Logging Setup
# ==================================
# Use simple key=value log format for readability and debugging.
# Designed for local development; can be extended to structured logging (JSON) in production.
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
    - persist raw events to local sinks
    - deduplicate events using Redis
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
    consumer = None

    for attempt in range(5):
        try:
            consumer = KafkaConsumer(
                TOPIC_SALES,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="sales-consumer-group"
            )
            logger.info("Connected to Kafka on attempt %s", attempt + 1)
            break
        except NoBrokersAvailable:
            logger.warning("Kafka not ready, retrying... attempt=%s/5", attempt + 1)
            time.sleep(3)

    if consumer is None:
        raise RuntimeError("Kafka broker is still not available after retries")

    # ==================================
    # Kafka Producer Setup (Derived Events)
    # ==================================
    # Publish alert and duplicate events to downstream Kafka topics.
    alert_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
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
    # Raw event sink
    # This file stores all received events before deduplication.
    # It may contain duplicate events by design.
    raw_event_file = event_dir / "raw_sales_events.jsonl"

    # Aggregation output
    aggregate_file = aggregates_dir / "sales_by_region.jsonl"

    # Consumer-level metrics
    metrics_file = metric_dir / "metrics.json"

    # Global metrics across all consumers
    global_metrics_file = metric_dir / "global_metrics.json"

    # Alert sinks
    alert_file = alerts_dir / "high_value_sales.jsonl"
    risky_alert_file = alerts_dir / "risky_discount_profit.jsonl"
    duplicate_file = alerts_dir / "duplicate_events.jsonl"
    failed_file = alerts_dir / "failed_events.jsonl"

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
        "events_received": 0,
        "events_aggregated": 0,
        "duplicates_skipped": 0,
        "alerts_triggered": 0,
        "events_failed": 0,
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
            "events_received_total": int(redis_client.get("metrics:events_received_total") or 0),
            "events_aggregated_total": int(redis_client.get("metrics:events_aggregated_total") or 0),
            "duplicates_skipped_total": int(redis_client.get("metrics:duplicates_skipped_total") or 0),
            "alerts_triggered_total": int(redis_client.get("metrics:alerts_triggered_total") or 0),
            "events_failed_total": int(redis_client.get("metrics:events_failed_total") or 0),
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
    logger.info("%s writing raw events to %s", consumer_name, raw_event_file)
    logger.info("%s writing aggregates to %s", consumer_name, aggregate_file)
    logger.info("%s writing metrics to %s", consumer_name, metrics_file)
    logger.info("%s writing global metrics to %s", consumer_name, global_metrics_file)
    logger.info("%s writing alerts to %s", consumer_name, alert_file)
    logger.info("%s writing duplicate skips to %s", consumer_name, duplicate_file)
    logger.info("%s writing failed events to %s", consumer_name, failed_file)
    logger.info("%s writing risky discount-profit alerts to %s", consumer_name, risky_alert_file)
    logger.info("%s publishing alerts to Kafka topic %s", consumer_name, TOPIC_ALERTS)
    logger.info("%s publishing duplicates to Kafka topic %s", consumer_name, TOPIC_DUPLICATES)
    logger.info("%s writing staging events to %s", consumer_name, STAGING_FILE)

    # ==================================
    # Main Consumer Loop
    # ==================================
    # Continuously process events from Kafka.
    for message in consumer:
        try:
            event = message.value
            partition = message.partition
            offset = message.offset
            topic = message.topic

            event_id = event.get("event_id")
            order_id = str(event["order_id"])
            region = event["region"]
            category = event.get("category")
            sub_category = event.get("sub_category")
            sales = float(event["sales"])
            profit = float(event.get("profit", 0.0))
            discount = float(event.get("discount", 0.0))
            event_time = event.get("event_time")

            if not event_id or str(event_id).strip() == "":
                logger.error("Missing event_id")
                append_jsonl(path="alerts/failed_events.jsonl", record=event)
                continue

            dedup_key = f"event:{event_id}"
            is_duplicate = bool(redis_client.exists(dedup_key))

            if is_duplicate:
                logger.warning(f"Duplicate event skipped: {event_id}")
                continue

            # ==================================
            # Event Processing
            # ==================================
            # Record all received events first (raw layer)
            # Deduplication is applied after raw persistence
            metrics["events_received"] += 1
            redis_client.incr("metrics:events_received_total")

            enriched_event = {
                "event_id": event_id,
                "consumer_name": f"consumer-{consumer_name}",
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "order_id": order_id,
                "region": region,
                "category": category,
                "sub_category": sub_category,
                "sales": sales,
                "profit": profit,
                "discount": discount,
                "event_time": event_time,
            }

            # Structured processed-event log
            # Log important routing and business fields in a stable format.
            logger.info(
                "event received | consumer=%s topic=%s partition=%s offset=%s order_id=%s region=%s sales=%.2f profit=%.2f discount=%.2f",
                consumer_name, topic, partition, offset, order_id, region, sales, profit, discount
            )

            with raw_event_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(enriched_event, ensure_ascii=False) + "\n")

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
                    "duplicate skipped | consumer=%s topic=%s partition=%s offset=%s order_id=%s region=%s sales=%s store=redis",
                    consumer_name, topic, partition, offset, order_id, region, sales
                )

                alert_producer.send(
                    topic=TOPIC_DUPLICATES,
                    key=order_id.encode(),
                    value=duplicate_event
                )
                alert_producer.flush()

                with duplicate_file.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(duplicate_event, ensure_ascii=False) + "\n")

                continue

            # ==================================
            # Aggregation (Sales by Region)
            # ==================================
            # Maintain an in-memory running total by region.
            if region not in sales_by_region:
                sales_by_region[region] = {
                    "total_sales": 0.0,
                    "events_aggregated": 0
                }

            sales_by_region[region]["total_sales"] += sales
            sales_by_region[region]["events_aggregated"] += 1

            metrics["events_aggregated"] += 1
            redis_client.incr("metrics:events_aggregated_total")

            aggregate_snapshot = {
                "updated_at": datetime.now(UTC).isoformat(),
                "consumer": consumer_name,
                "region": region,
                "total_sales": round(sales_by_region[region]["total_sales"], 2),
                "events_aggregated": sales_by_region[region]["events_aggregated"],
            }

            with aggregate_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(aggregate_snapshot, ensure_ascii=False) + "\n")

            # ==================================
            # Alert Detection (Risky Discount-Profit)
            # ==================================
            # Trigger an alert when discount is high
            # but profit is low or negative.
            if discount >= HIGH_DISCOUNT_THRESHOLD and profit <= LOW_PROFIT_THRESHOLD:
                metrics["alerts_triggered"] += 1
                redis_client.incr("metrics:alerts_triggered_total")

                risky_alert_event = {
                    "alert_type": "risky_discount_profit",
                    "detected_at": datetime.now(UTC).isoformat(),
                    "consumer": consumer_name,
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "order_id": order_id,
                    "region": region,
                    "category": category,
                    "sub_category": sub_category,
                    "sales": sales,
                    "profit": profit,
                    "discount": discount,
                    "event_time": event_time,
                }

                logger.warning(
                    "risky discount-profit detected | consumer=%s topic=%s partition=%s offset=%s order_id=%s region=%s category=%s sub_category=%s sales=%.2f profit=%.2f discount=%.2f",
                    consumer_name,
                    topic,
                    partition,
                    offset,
                    order_id,
                    region,
                    category,
                    sub_category,
                    sales,
                    profit,
                    discount,
                )

                with risky_alert_file.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(risky_alert_event, ensure_ascii=False) + "\n")

                alert_producer.send(
                    topic=TOPIC_ALERTS,
                    key=region.encode(),
                    value=risky_alert_event
                )
                alert_producer.flush()

                # Telegram alert
                message = (
                    "⚠️ Risky discount-profit event\n"
                    f"consumer: {consumer_name}\n"
                    f"order_id: {event['order_id']}\n"
                    f"region: {event['region']}\n"
                    f"category: {event['category']}\n"
                    f"sub_category: {event['sub_category']}\n"
                    f"sales: {event['sales']}\n"
                    f"profit: {event['profit']}\n"
                    f"discount: {event['discount']}\n"
                    f"time: {event['event_time']}"
                )
                send_telegram_alert(message)

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
                    "high value sale detected | consumer=%s topic=%s partition=%s offset=%s order_id=%s region=%s sales=%s",
                    consumer_name, topic, partition, offset, order_id, region, sales
                )

                with alert_file.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(alert_event, ensure_ascii=False) + "\n")

                alert_producer.send(
                    topic=TOPIC_ALERTS,
                    key=region.encode(),
                    value=alert_event
                )
                alert_producer.flush()

                # Telegram alert
                message = (
                    "🚨 High-value sale detected\n"
                    f"consumer: {consumer_name}\n"
                    f"order_id: {event['order_id']}\n"
                    f"region: {event['region']}\n"
                    f"category: {event['category']}\n"
                    f"sub_category: {event['sub_category']}\n"
                    f"sales: {event['sales']}\n"
                    f"profit: {event['profit']}\n"
                    f"discount: {event['discount']}\n"
                    f"time: {event['event_time']}"
                )
                send_telegram_alert(message)

            # ==================================
            # Build + Write Staging (NEW)
            # ==================================
            is_high_value = sales > HIGH_VALUE_THRESHOLD

            staging_record = build_staging_record(
                event=enriched_event,
                is_duplicate=is_duplicate,
                is_high_value=is_high_value,
                consumer_name=consumer_name
            )

            append_jsonl(STAGING_FILE, staging_record)

            logger.info(
                "staged event | consumer=%s event_id=%s staging_file=%s",
                consumer_name,
                staging_record.get("event_id"),
                STAGING_FILE
            )

            redis_client.setex(dedup_key, DEDUP_TTL_SECONDS, "1")

            # ==================================
            # Persist Metrics
            # ==================================
            # Persist local and global metric snapshots after each processed event.
            write_metrics()
            write_global_metrics()

        except Exception as e:
            metrics["events_failed"] += 1
            redis_client.incr("metrics:events_failed_total")

            failed_event = {
                "failed_at": datetime.now(UTC).isoformat(),
                "consumer": consumer_name,
                "topic": getattr(message, "topic", None),
                "partition": getattr(message, "partition", None),
                "offset": getattr(message, "offset", None),
                "raw_value": str(getattr(message, "value", None)),
                "error": str(e),
            }

            logger.exception(
                "event processing failed | consumer=%s topic=%s partition=%s offset=%s",
                consumer_name,
                getattr(message, "topic", None),
                getattr(message, "partition", None),
                getattr(message, "offset", None),
            )

            with failed_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(failed_event, ensure_ascii=False) + "\n")

            write_metrics()
            write_global_metrics()

            continue

# ==================================
# Script Entry Point
# ==================================
if __name__ == "__main__":
    import sys

    name = sys.argv[1] if len(sys.argv) > 1 else "consumer-A"
    main(name)