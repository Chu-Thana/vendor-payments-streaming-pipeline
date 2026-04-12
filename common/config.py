# Shared configuration for the local Kafka streaming project.

import os
from dotenv import load_dotenv

load_dotenv()

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
HIGH_VALUE_THRESHOLD = 400

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

print(TELEGRAM_CHAT_ID)