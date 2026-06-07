# Vendor Payments Streaming Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Streaming](https://img.shields.io/badge/Streaming-Kafka-orange)
![Deduplication](https://img.shields.io/badge/Deduplication-Redis-red)
![Container](https://img.shields.io/badge/Container-Docker-blue)
![Testing](https://img.shields.io/badge/Testing-pytest-green)
![Code Quality](https://img.shields.io/badge/Code%20Quality-Ruff-purple)

---

## Summary

This project implements a Kafka-based streaming pipeline using real Vendor Payments data.

It simulates a production-style streaming ingestion workflow where Vendor Payments records are converted into events, published to Kafka, intentionally duplicated to simulate retry/replay scenarios, consumed by a Kafka consumer, deduplicated with Redis, and written to a staging layer for downstream validation.

The main design principle is:

```text
Prevent data loss first, then handle duplicates safely.
```

This project is designed as the real-time ingestion layer of a broader Vendor Payments data platform.

---

## Role in the Data Engineering Portfolio

This project is **Project 3** in the Vendor Payments Data Engineering Portfolio.

It complements the completed batch track:

```text
Project 1: Vendor Payments Batch ETL Foundation
Project 3: Kafka Streaming Pipeline with Redis Deduplication
Project 4: Airflow Orchestration
Project 5: AWS S3 Data Lake + Athena Analytics
```

Project 3 focuses on streaming ingestion and first-level duplicate handling before downstream batch validation.

---

## Architecture

```text
Project 1 Silver Output
→ Streaming Sample Preparation
→ Vendor Payments Event Builder
→ Kafka Producer
→ Duplicate Injection
→ Kafka Topic
→ Kafka Consumer
→ Redis First-level Deduplication
→ Staging JSONL Output
→ Streaming Summary Report
→ Downstream Airflow Validation / Deduplication
```

---

## Data Flow

```text
Vendor Payments silver sample
→ data/input/vendor_payments_stream_sample.csv
→ producer publishes events to Kafka
→ duplicate events are intentionally injected
→ consumer reads from Kafka
→ Redis checks event_id for duplicates
→ accepted events are written to staging
→ duplicate events are rejected
→ summary report is generated
```

---

## Design Principle

Distributed streaming systems may deliver events more than once.

This project follows an **at-least-once processing mindset**:

```text
Prevent data loss first, then handle duplicates safely.
```

Kafka is used for reliable event transport, while Redis is used as a fast first-level deduplication store before writing accepted events to staging.

The goal is not to guarantee exactly-once processing at the ingestion layer. Instead, the pipeline is designed to avoid data loss first, then handle duplicates safely through explicit deduplication.

---

## Key Features

* Uses real Vendor Payments data from the batch ETL project
* Generates streaming input from Project 1 silver output
* Publishes Vendor Payments events to Kafka
* Intentionally injects duplicate events to simulate retry/replay
* Applies Redis-based first-level deduplication
* Writes accepted events to staging JSONL
* Generates streaming summary metrics
* Provides unit tests for event building, deduplication, writer, reporting, and consumer validation
* Uses Ruff and pytest in GitHub Actions CI

---

## Project Structure

```text
vendor-payments-streaming-pipeline/
│
├── common/
│   ├── config.py
│   ├── dedup.py
│   ├── event_builder.py
│   ├── reporting.py
│   └── writer.py
│
├── producer/
│   └── producer.py
│
├── consumer/
│   └── consumer.py
│
├── scripts/
│   ├── create_topic.ps1
│   └── prepare_stream_sample.py
│
├── data/
│   └── input/
│
├── output/
│   ├── staging/
│   └── reports/
│
├── tests/
│
├── docker-compose.yml
├── run_producer.py
├── run_consumer.py
├── requirements.txt
└── README.md
```

---

## Main Components

### 1. Streaming Sample Preparation

```text
scripts/prepare_stream_sample.py
```

This script reads the Vendor Payments silver output from Project 1 and prepares a streaming input sample.

It adds event-level fields such as:

```text
event_id
event_type
event_timestamp
source_system
```

The generated file is written to:

```text
data/input/vendor_payments_stream_sample.csv
```

---

### 2. Event Builder

```text
common/event_builder.py
```

The event builder converts each Vendor Payments row into a structured Kafka event.

Each event contains:

```text
event_id
event_type
event_timestamp
source_system
source_row_hash
business_composite_key
fiscal_year
vendor_name
department_name
payment_status
payment_amount
payload
```

The full row is preserved inside the payload so downstream systems can still access the original event data.

---

### 3. Kafka Producer

```text
producer/producer.py
```

The producer reads the prepared streaming sample, builds Vendor Payments events, intentionally injects duplicate events, and publishes them to Kafka.

Duplicate events reuse the same `event_id` to simulate real retry/replay scenarios.

Kafka topic:

```text
vendor_payments_events
```

---

### 4. Redis Deduplication

```text
common/dedup.py
```

Redis is used as a first-level deduplication store.

Deduplication key format:

```text
event:{event_id}
```

If the event ID already exists in Redis, the event is rejected as a duplicate.

If the event ID does not exist, the event is accepted and marked as processed.

---

### 5. Kafka Consumer

```text
consumer/consumer.py
```

The consumer reads events from Kafka, validates required event fields, applies Redis deduplication, writes accepted events to staging, rejects duplicate events, and generates summary metrics.

The consumer uses manual offset commit after processing each event.

This supports an at-least-once processing design where data loss is avoided first, and duplicates are handled safely by Redis.

---

### 6. Staging Writer

```text
common/writer.py
```

Accepted events are written to:

```text
output/staging/vendor_payments_streaming_staging.jsonl
```

Each accepted event includes:

```text
dedup_status
ingested_at
```

The staging output is designed for downstream validation and secondary deduplication by the batch/orchestration layer.

---

### 7. Summary Reporting

```text
common/reporting.py
```

The streaming summary report is written to:

```text
output/reports/streaming_summary_report.json
```

The report includes:

```text
base_events
produced_events
accepted_events
rejected_duplicates
duplicate_rate
dedup_strategy
staging_output
principle
```

---

## Local Setup

Start Kafka, Zookeeper, and Redis:

```bash
docker compose up -d
```

Prepare the streaming sample from Project 1 silver output:

```bash
python scripts/prepare_stream_sample.py
```

Run the producer:

```bash
python run_producer.py
```

Clear Redis before a clean consumer run:

```bash
docker exec redis redis-cli FLUSHDB
```

Run the consumer:

```bash
python run_consumer.py
```

---

## Example Development Run

In the current development run, the project used a 10,000-row streaming sample.

The producer intentionally injected 5% duplicate events:

```text
Base events: 10,000
Duplicate events injected: 500
Total events produced: 10,500
Duplicate rate configured: 0.0500
```

The consumer processed the stream and applied Redis deduplication:

```text
Consumed events: 10,500
Accepted events: 10,000
Rejected duplicates: 500
Failed events: 0
```

Generated report:

```text
output/reports/streaming_summary_report.json
```

Generated staging output:

```text
output/staging/vendor_payments_streaming_staging.jsonl
```

---

## Streaming Summary Report Example

```json
{
  "topic": "vendor_payments_events",
  "base_events": 10000,
  "produced_events": 10500,
  "accepted_events": 10000,
  "rejected_duplicates": 500,
  "duplicate_rate": 0.0476,
  "dedup_strategy": "redis_event_id_first_level_dedup",
  "staging_output": "output\\staging\\vendor_payments_streaming_staging.jsonl",
  "principle": "Prevent data loss first, then handle duplicates safely."
}
```

---

## Testing

Run Ruff:

```bash
python -m ruff check .
```

Run tests:

```bash
python -m pytest -v
```

Current test coverage includes:

* project structure validation
* Vendor Payments event builder
* Redis deduplication helper
* staging writer
* streaming summary reporting
* consumer event validation

Current result:

```text
12 passed
```

---

## CI

GitHub Actions validates:

```text
Ruff lint
pytest
Docker Compose config
```

The CI workflow ensures that the streaming project remains testable and maintainable without requiring Kafka or Redis to run inside the unit test suite.

---

## Current Status

Completed:

```text
Vendor Payments config refactor
Streaming sample preparation
Vendor Payments event builder
Kafka producer with duplicate injection
Redis first-level deduplication helper
Kafka consumer with Redis deduplication
Staging JSONL writer
Streaming summary report
Unit tests
Ruff validation
GitHub Actions CI
```

Development run completed successfully:

```text
Produced events: 10,500
Accepted events: 10,000
Rejected duplicates: 500
Failed events: 0
```

---

## Next Improvements

Planned improvements:

```text
Run final 100,000-row streaming simulation
Capture updated evidence screenshots
Add optional large-payment alerting
Update README with final screenshots
Prepare pull request and merge into main
```

Optional alerting can be added later using Vendor Payments business rules, such as large payment detection.

---

## What This Project Demonstrates

This project demonstrates practical streaming data engineering patterns:

* Kafka producer and consumer design
* Event-driven ingestion
* At-least-once processing mindset
* Duplicate injection for reliability testing
* Redis first-level deduplication
* Staging output for downstream batch validation
* Summary metrics for observability
* Unit testing and CI validation

This is not only a Kafka demo. It is a streaming ingestion layer designed to integrate with a broader data platform.
