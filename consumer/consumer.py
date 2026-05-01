import json
import logging
import time
from pathlib import Path
from datetime import datetime, UTC, timezone

import redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

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
    STAGING_FILE,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM,
    KAFKA_USERNAME,
    KAFKA_PASSWORD,
    LOG_LEVEL,
    RISKY_SALES_THRESHOLD,
    RISKY_PROFIT_THRESHOLD,
    RISKY_DISCOUNT_THRESHOLD,
    TOPIC_ALERTS_CRITICAL,
    TOPIC_ALERTS_WARNING,
)

# ==================================
# Helper functions
# ==================================
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def parse_event_time(event_time: str):
    try:
        dt = datetime.fromisoformat(event_time)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except:
        return None

def append_jsonl(file_path: Path, record: dict) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def build_staging_record(event: dict, is_duplicate: bool, is_high_value: bool, consumer_name: str) -> dict:
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
        "is_duplicate": int(is_duplicate),
        "is_high_value": int(is_high_value),
        "consumer_name": f"consumer-{consumer_name}",
        "pipeline_stage": "staged",
    }


def build_kafka_consumer() -> KafkaConsumer:
    config = {
        "bootstrap_servers": KAFKA_BROKER,
        "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
        "group_id": "sales-consumer-group-v3",
    }

    if KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        config.update({
            "security_protocol": KAFKA_SECURITY_PROTOCOL,
            "sasl_mechanism": KAFKA_SASL_MECHANISM,
            "sasl_plain_username": KAFKA_USERNAME,
            "sasl_plain_password": KAFKA_PASSWORD,
        })

    return KafkaConsumer(TOPIC_SALES, **config)


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


# ==================================
# Logging Setup
# ==================================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def main(consumer_name: str) -> None:
    """
    Start a Kafka consumer instance for processing sales events.
    """

    consumer = None

    for attempt in range(5):
        try:
            consumer = build_kafka_consumer()
            logger.info(
                "Connected to Kafka | broker=%s | security_protocol=%s | attempt=%s",
                KAFKA_BROKER,
                KAFKA_SECURITY_PROTOCOL,
                attempt + 1,
            )
            break
        except NoBrokersAvailable:
            logger.warning("Kafka not ready, retrying... attempt=%s/5", attempt + 1)
            time.sleep(3)

    if consumer is None:
        raise RuntimeError("Kafka broker is still not available after retries")

    alert_producer = build_kafka_producer()

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )

    event_dir = Path("output/event")
    event_dir.mkdir(parents=True, exist_ok=True)

    aggregates_dir = Path("output/aggregates")
    aggregates_dir.mkdir(parents=True, exist_ok=True)

    metric_dir = Path("output/metric")
    metric_dir.mkdir(parents=True, exist_ok=True)

    alerts_dir = Path("alerts")
    alerts_dir.mkdir(parents=True, exist_ok=True)

    raw_event_file = event_dir / "raw_sales_events.jsonl"
    aggregate_file = aggregates_dir / "sales_by_region.jsonl"
    metrics_file = metric_dir / f"metrics_{consumer_name}.json"
    global_metrics_file = metric_dir / "global_metrics.json"
    alert_file = alerts_dir / "alerts.jsonl"
    risky_alert_file = alerts_dir / "risky_discount_profit.jsonl"
    duplicate_file = alerts_dir / "duplicate_events.jsonl"
    failed_file = alerts_dir / "failed_events.jsonl"

    sales_by_region = {}

    metrics = {
        "consumer": consumer_name,
        "events_total": 0,
        "events_received": 0,
        "events_aggregated": 0,
        "duplicates_skipped": 0,
        "alerts_triggered": 0,
        "events_failed": 0,
        "throughput_events_per_sec": 0.0,
        "avg_latency_sec": 0.0,
        "duplicate_rate_pct": 0.0,
        "last_updated": None,

        "alert_breakdown": {
            "high_value": 0,
            "high_discount": 0,
            "low_profit": 0,
            "risky": 0,
            "warning": 0,
            "critical": 0
        }
    }
    benchmark_start_time = time.time()
    latencies = []

    def write_metrics() -> None:
        metrics["last_updated"] = datetime.now(UTC).isoformat()
        with metrics_file.open("w", encoding="utf-8") as fo:
            json.dump(metrics, fo, ensure_ascii=False, indent=2)

    def write_global_metrics() -> None:
        events_received_total = int(redis_client.get("metrics:events_received_total") or 0)
        events_aggregated_total = int(redis_client.get("metrics:events_aggregated_total") or 0)
        duplicates_skipped_total = int(redis_client.get("metrics:duplicates_skipped_total") or 0)
        alerts_triggered_total = int(redis_client.get("metrics:alerts_triggered_total") or 0)
        alert_high_value_total = int(redis_client.get("metrics:alert_high_value_total") or 0)
        alert_high_discount_total = int(redis_client.get("metrics:alert_high_discount_total") or 0)
        alert_low_profit_total = int(redis_client.get("metrics:alert_low_profit_total") or 0)
        events_failed_total = int(redis_client.get("metrics:events_failed_total") or 0)
        alert_risky_total = int(redis_client.get("metrics:alert_risky_total") or 0)
        alert_warning_total = int(redis_client.get("metrics:alert_warning_total") or 0)
        alert_critical_total = int(redis_client.get("metrics:alert_critical_total") or 0)

        events_total = events_received_total + duplicates_skipped_total

        duplicate_rate_pct = (
            round(duplicates_skipped_total / events_total * 100, 2)
            if events_total > 0
            else 0.0
        )

        global_metrics = {
            "events_total": events_total,
            "events_received_total": events_received_total,
            "events_aggregated_total": events_aggregated_total,
            "duplicates_skipped_total": duplicates_skipped_total,
            "alerts_triggered_total": alerts_triggered_total,
            "events_failed_total": events_failed_total,
            "duplicate_rate_pct": duplicate_rate_pct,
            "last_updated": datetime.now(UTC).isoformat(),
            "alert_breakdown": {
                "high_value": alert_high_value_total,
                "high_discount": alert_high_discount_total,
                "low_profit": alert_low_profit_total,
                "risky": alert_risky_total,
                "warning": alert_warning_total,
                "critical": alert_critical_total
            }
        }

        total_alerts = alerts_triggered_total
        total_events = events_received_total
        critical = alert_critical_total

        global_metrics["critical_ratio"] = round(
            (critical / total_alerts) * 100, 2
        ) if total_alerts > 0 else 0

        global_metrics["alert_rate"] = round(
            (total_alerts / total_events) * 100, 2
        ) if total_events > 0 else 0

        with global_metrics_file.open("w", encoding="utf-8") as fx:
            json.dump(global_metrics, fx, ensure_ascii=False, indent=2)

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

    for message in consumer:
        metrics["events_total"] += 1
        try:
            event = message.value
            partition = message.partition
            offset = message.offset
            topic = message.topic

            required_fields = [
                "event_id",
                "order_id",
                "region",
                "sales",
                "profit",
                "discount",
                "event_time",
            ]
            missing_fields = [f for f in required_fields if f not in event]

            if missing_fields:
                raise ValueError(f"missing required fields: {missing_fields}")

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
                append_jsonl(failed_file, event)
                continue

            dedup_key = f"event:{event_id}"
            is_duplicate = bool(redis_client.exists(dedup_key))

            if is_duplicate:
                metrics["duplicates_skipped"] += 1
                redis_client.incr("metrics:duplicates_skipped_total")

                if metrics["events_total"] > 0:
                    metrics["duplicate_rate_pct"] = round(
                        metrics["duplicates_skipped"] / metrics["events_total"] * 100,
                        2
                    )

                write_metrics()
                write_global_metrics()

                logger.warning("Duplicate event skipped: %s", event_id)
                continue

            # non-duplicate
            metrics["events_received"] += 1
            redis_client.incr("metrics:events_received_total")

            # ---- benchmark metrics ----
            elapsed = time.time() - benchmark_start_time
            if elapsed > 0:
                metrics["throughput_events_per_sec"] = round(metrics["events_received"] / elapsed, 2)

            event_dt = parse_event_time(event_time)
            if event_dt is not None:
                latency_sec = (datetime.now(timezone.utc) - event_dt).total_seconds()
                latencies.append(latency_sec)

                if len(latencies) > 1000:
                    latencies.pop(0)

                metrics["avg_latency_sec"] = round(sum(latencies) / len(latencies), 2)

            if metrics["events_received"] > 0:
                metrics["duplicate_rate_pct"] = round(
                    metrics["duplicates_skipped"] / metrics["events_received"] * 100,
                    2
                )

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

            logger.info(
                "event received | consumer=%s topic=%s partition=%s offset=%s order_id=%s region=%s sales=%.2f profit=%.2f discount=%.2f",
                consumer_name, topic, partition, offset, order_id, region, sales, profit, discount
            )

            append_jsonl(raw_event_file, enriched_event)

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

                if metrics["events_received"] > 0:
                    metrics["duplicate_rate_pct"] = round(
                        metrics["duplicates_skipped"] / metrics["events_received"] * 100,
                        2
                    )

                if metrics["events_received"] % 1000 == 0:
                    logger.info(
                        "streaming benchmark | events=%s throughput=%s events/sec avg_latency=%s sec duplicate_rate=%s%%",
                        metrics["events_received"],
                        metrics["throughput_events_per_sec"],
                        metrics["avg_latency_sec"],
                        metrics["duplicate_rate_pct"],
                    )

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

                append_jsonl(duplicate_file, duplicate_event)
                continue

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

            append_jsonl(aggregate_file, aggregate_snapshot)

            if (
                    sales >= RISKY_SALES_THRESHOLD
                    and discount >= RISKY_DISCOUNT_THRESHOLD
                    and profit <= RISKY_PROFIT_THRESHOLD
            ):
                metrics["alerts_triggered"] += 1
                metrics["alert_breakdown"]["risky"] += 1
                metrics["alert_breakdown"]["critical"] += 1

                redis_client.incr("metrics:alerts_triggered_total")
                redis_client.incr("metrics:alert_risky_total")
                redis_client.incr("metrics:alert_critical_total")

                risky_alert_event = {
                    "alert_type": "risky_discount_profit",
                    "severity": "critical",
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
                    "alert_breakdown": {
                        "high_value": int(sales >= HIGH_VALUE_THRESHOLD),
                        "high_discount": int(discount >= HIGH_DISCOUNT_THRESHOLD),
                        "low_profit": int(profit <= LOW_PROFIT_THRESHOLD),
                        "risky": 1
                    }
                }

                logger.error(
                    "🚨 CRITICAL alert detected | consumer=%s topic=%s partition=%s offset=%s order_id=%s region=%s sales=%.2f profit=%.2f discount=%.2f",
                    consumer_name,
                    topic,
                    partition,
                    offset,
                    order_id,
                    region,
                    sales,
                    profit,
                    discount,
                )

                append_jsonl(risky_alert_file, risky_alert_event)

                alert_producer.send(
                    topic=TOPIC_ALERTS_CRITICAL,
                    key=region.encode(),
                    value=risky_alert_event
                )
                alert_producer.flush()

                message_text = (
                    "🚨 CRITICAL ALERT\n"
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
                send_telegram_alert(message_text)

            alert_types = []

            if sales >= HIGH_VALUE_THRESHOLD:
                metrics["alerts_triggered"] += 1
                metrics["alert_breakdown"]["high_value"] += 1
                redis_client.incr("metrics:alerts_triggered_total")
                redis_client.incr("metrics:alert_high_value_total")
                alert_types.append("high_value")

            if discount >= HIGH_DISCOUNT_THRESHOLD:
                metrics["alerts_triggered"] += 1
                metrics["alert_breakdown"]["high_discount"] += 1
                redis_client.incr("metrics:alerts_triggered_total")
                redis_client.incr("metrics:alert_high_discount_total")
                alert_types.append("high_discount")

            if profit <= LOW_PROFIT_THRESHOLD:
                metrics["alerts_triggered"] += 1
                metrics["alert_breakdown"]["low_profit"] += 1
                redis_client.incr("metrics:alerts_triggered_total")
                redis_client.incr("metrics:alert_low_profit_total")
                alert_types.append("low_profit")

            if alert_types:

                metrics["alert_breakdown"]["warning"] += 1
                redis_client.incr("metrics:alert_warning_total")

                alert_event = {
                    "alert_type": ",".join(alert_types),
                    "severity": "warning",
                    "detected_at": datetime.now(UTC).isoformat(),
                    "consumer": consumer_name,
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "order_id": event["order_id"],
                    "region": region,
                    "sales": sales,
                }

                append_jsonl(alert_file, alert_event)

                logger.warning(
                    "⚠️ WARNING alert detected | consumer=%s topic=%s partition=%s offset=%s order_id=%s region=%s alert_types=%s sales=%.2f",
                    consumer_name,
                    topic,
                    partition,
                    offset,
                    order_id,
                    region,
                    alert_types,
                    sales,
                )

                append_jsonl(alert_file, alert_event)

                alert_producer.send(
                    topic=TOPIC_ALERTS_WARNING,
                    key=region.encode(),
                    value=alert_event
                )
                alert_producer.flush()

                message_text = (
                    "⚠️ WARNING ALERT\n"
                    "High value / discount / low profit detected\n"
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
                send_telegram_alert(message_text)

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

            logger.error("failed raw event: %s", getattr(message, "value", None))

            logger.exception(
                "event processing failed | error=%s | consumer=%s topic=%s partition=%s offset=%s",
                str(e),
                consumer_name,
                getattr(message, "topic", None),
                getattr(message, "partition", None),
                getattr(message, "offset", None),
            )

            append_jsonl(failed_file, failed_event)

            write_metrics()
            write_global_metrics()
            continue


if __name__ == "__main__":
    import sys

    name = sys.argv[1] if len(sys.argv) > 1 else "consumer-A"
    main(name)