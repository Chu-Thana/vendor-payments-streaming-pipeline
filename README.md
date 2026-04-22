# 🚀 Kafka Streaming Pipeline (Project 3)

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Kafka](https://img.shields.io/badge/Streaming-Kafka-orange)
![Confluent](https://img.shields.io/badge/Kafka-Confluent_Cloud-purple)
![Redis](https://img.shields.io/badge/Deduplication-Redis-red)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow-green)

> Production-style real-time streaming pipeline with **Kafka, Redis, and Airflow**

---

## 📸 System Overview
![Kafka Flow](assets/02_kafka_event_flow.png)

---

# 🏗 Architecture Overview

```mermaid
flowchart LR

subgraph Streaming
    Producer --> Kafka --> Consumer --> RawData
end

subgraph Batch
    RawData --> ETL --> Warehouse
end

subgraph Serving
    Warehouse --> API --> Client
end

subgraph Orchestration
    Airflow --> ETL
    Airflow --> Monitoring
end
```

---

# 📸 Pipeline Walkthrough

## 1️⃣ Kafka Topics
![Kafka Topics](assets/01_kafka_topics_overview.png)

---

## 2️⃣ Event Flow
![Kafka Flow](assets/02_kafka_event_flow.png)

---

## 3️⃣ Consumer Processing
![Consumer](assets/03_consumer_processing_log.png)

---

## 4️⃣ Staging Output
![Staging](assets/04_staging_output_data.png)

---

## 5️⃣ Duplicate Simulation
![Duplicate Producer](assets/05_duplicate_event_producer.png)

---

## 6️⃣ Deduplication (Consumer)
![Duplicate Detection](assets/06_duplicate_detection_consumer.png)

---

## 7️⃣ Real-time Alerts
![Alert](assets/07_realtime_alert_telegram.png)

---

# ⚙️ Features

- Kafka real-time streaming (Confluent Cloud)
- Consumer group processing
- Redis-based deduplication
- Real-time alert detection
- JSONL staging output
- Airflow batch integration

---

# 🧠 Key Concepts

- Event-driven architecture
- At-least-once processing
- Partition-based scaling
- Idempotent processing
- Streaming + Batch hybrid design

---

# ▶️ Run

```bash
python run_consumer.py consumer-A
python run_producer.py
python run_producer_duplicates.py
```

---

# 📌 Summary

This project demonstrates an **end-to-end streaming pipeline**:

**Ingestion → Processing → Deduplication → Alerting → Batch Integration**

Designed to reflect real-world data engineering systems.
