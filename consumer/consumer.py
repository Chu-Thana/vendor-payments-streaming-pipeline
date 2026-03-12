from kafka import KafkaConsumer
import json
import sys
from pathlib import Path
from datetime import datetime, UTC
import redis

consumer_name = sys.argv[1] if len(sys.argv) > 1 else "consumer-unknown"

consumer = KafkaConsumer(
    "sales_events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="sales-consumer-group"
)

# Redis client
redis_client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

output_dir = Path("output")
output_dir.mkdir(exist_ok=True)

alerts_dir = Path("alerts")
alerts_dir.mkdir(exist_ok=True)

output_file = output_dir / "sales_events.jsonl"
aggregate_file = output_dir / "sales_by_region.jsonl"
metrics_file = output_dir / "metrics.json"
global_metrics_file = output_dir / "global_metrics.json"
alert_file = alerts_dir / "high_value_sales.jsonl"
duplicate_file = alerts_dir / "duplicate_events.jsonl"

sales_by_region = {}

metrics = {
    "consumer": consumer_name,
    "events_processed": 0,
    "duplicates_skipped": 0,
    "alerts_triggered": 0,
    "last_updated": None
}

def write_metrics() -> None:
    metrics["last_updated"] = datetime.now(UTC).isoformat()

    with metrics_file.open("w", encoding="utf-8") as fo:
        json.dump(metrics, fo, ensure_ascii=False, indent=2)

def write_global_metrics() -> None:
    global_metrics = {
        "events_processed_total": int(redis_client.get("metrics:events_processed_total") or 0),
        "duplicates_skipped_total": int(redis_client.get("metrics:duplicates_skipped_total") or 0),
        "alerts_triggered_total": int(redis_client.get("metrics:alerts_triggered_total") or 0),
        "last_updated": datetime.now(UTC).isoformat()
    }

    with global_metrics_file.open("w", encoding="utf-8") as fx:
        json.dump(global_metrics, fx, ensure_ascii=False, indent=2)

print(f"{consumer_name} started and waiting for messages...")
print(f"{consumer_name} writing events to {output_file}")
print(f"{consumer_name} writing aggregates to {aggregate_file}")
print(f"{consumer_name} writing alerts to {alert_file}")
print(f"{consumer_name} writing duplicate skips to {duplicate_file}")

for message in consumer:
    event = message.value
    partition = message.partition
    offset = message.offset
    topic = message.topic
    order_id = str(event["order_id"])

    redis_key = f"processed_order:{order_id}"

    # Redis SETNX behavior via set(..., nx=True)
    was_set = redis_client.set(redis_key, "1", nx=True)

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
            "region": event["region"],
            "sales": event["sales"],
            "reason": "duplicate_order_id_skipped_redis"
        }

        print(f"{consumer_name} duplicate skipped (redis): order_id={order_id}")

        with duplicate_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(duplicate_event, ensure_ascii=False) + "\n")

        continue

    metrics["events_processed"] += 1
    redis_client.incr("metrics:events_processed_total")

    enriched_event = {
        "consumer": consumer_name,
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "order_id": event["order_id"],
        "region": event["region"],
        "sales": event["sales"],
    }

    print(
        f"{consumer_name} | topic={topic} | partition={partition} | offset={offset} | event={event}"
    )

    with output_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(enriched_event, ensure_ascii=False) + "\n")

    region = event["region"]
    sales = event["sales"]

    if region not in sales_by_region:
        sales_by_region[region] = {
            "total_sales": 0.0,
            "events_processed": 0
        }

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

    if event["sales"] > 400:
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
            "region": event["region"],
            "sales": event["sales"],
        }

        print(f"{consumer_name} 🔥 High value sale detected!")

        with alert_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(alert_event, ensure_ascii=False) + "\n")

    write_metrics()
    write_global_metrics()