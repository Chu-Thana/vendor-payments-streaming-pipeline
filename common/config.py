# Shared configuration for the local Kafka streaming project.

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ==================================
# Staging file
# ==================================
BASE_DIR = Path(__file__).resolve().parents[1]

OUTPUT_DIR = BASE_DIR / "output"
EVENT_DIR = OUTPUT_DIR / "event"
AGGREGATE_DIR = OUTPUT_DIR / "aggregates"
METRIC_DIR = OUTPUT_DIR / "metric"
ALERT_DIR = BASE_DIR / "alerts"

STAGING_DIR = OUTPUT_DIR / "staging"
STAGING_FILE = Path(
    os.getenv(
        "STAGING_FILE",
        str(STAGING_DIR / "staging_sales_events.jsonl")
    )
)
# ==================================
# Kafka
# ==================================
KAFKA_BROKER = "localhost:9092"

# Main business event topic
TOPIC_SALES = "sales_events"

# Derived / alert topics
TOPIC_ALERTS = "sales_alerts"
TOPIC_DUPLICATES = "duplicate_events"

TOPIC_HIGH_VALUE_ALERTS = "high_value_alerts"
TOPIC_RISKY_DISCOUNT_ALERTS = "risky_discount_profit_alerts"

# ==================================
# Business rule thresholds
# ==================================
HIGH_VALUE_THRESHOLD = 15000

HIGH_DISCOUNT_THRESHOLD = 0.30
LOW_PROFIT_THRESHOLD = 0.0

# ==================================
# Redis
# ==================================
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Redis deduplication window (seconds)
DEDUP_TTL_SECONDS = 3600

# ==================================
# Logging
# ==================================
LOG_LEVEL = "INFO"
KAFKA_LOG_LEVEL = "WARNING"
REDIS_LOG_LEVEL = "WARNING"

# ==================================
# Telegram
# ==================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ENABLE_TELEGRAM_ALERTS = os.getenv("ENABLE_TELEGRAM_ALERTS", "false").lower() == "true"
