import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ==================================
# Base paths
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
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")

# Main business event topic
TOPIC_SALES = os.getenv("TOPIC_SALES", "sales_events")

# Derived / alert topics
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "sales_alerts")
TOPIC_DUPLICATES = os.getenv("TOPIC_DUPLICATES", "duplicate_events")

TOPIC_HIGH_VALUE_ALERTS = os.getenv("TOPIC_HIGH_VALUE_ALERTS", "high_value_alerts")
TOPIC_RISKY_DISCOUNT_ALERTS = os.getenv(
    "TOPIC_RISKY_DISCOUNT_ALERTS",
    "risky_discount_profit_alerts"
)

TOPIC_ALERTS_CRITICAL = os.getenv("TOPIC_ALERTS_CRITICAL", "sales_alerts_critical")
TOPIC_ALERTS_WARNING = os.getenv("TOPIC_ALERTS_WARNING", "sales_alerts_warning")

# ==================================
# Business rule thresholds
# ==================================
HIGH_VALUE_THRESHOLD = float(os.getenv("HIGH_VALUE_THRESHOLD", "15000"))
HIGH_DISCOUNT_THRESHOLD = float(os.getenv("HIGH_DISCOUNT_THRESHOLD", "0.30"))
LOW_PROFIT_THRESHOLD = float(os.getenv("LOW_PROFIT_THRESHOLD", "0.0"))

# ==================================
# Redis
# ==================================
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Redis deduplication window (seconds)
DEDUP_TTL_SECONDS = int(os.getenv("DEDUP_TTL_SECONDS", "3600"))

# ==================================
# Logging
# ==================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
KAFKA_LOG_LEVEL = os.getenv("KAFKA_LOG_LEVEL", "WARNING")
REDIS_LOG_LEVEL = os.getenv("REDIS_LOG_LEVEL", "WARNING")

# ==================================
# Telegram
# ==================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ENABLE_TELEGRAM_ALERTS = os.getenv("ENABLE_TELEGRAM_ALERTS", "false").lower() == "true"

# ==================================
# RISKY
# ==================================
RISKY_SALES_THRESHOLD = float(os.getenv("RISKY_SALES_THRESHOLD", 10000))
RISKY_PROFIT_THRESHOLD = float(os.getenv("RISKY_PROFIT_THRESHOLD", 100))
RISKY_DISCOUNT_THRESHOLD = float(os.getenv("RISKY_DISCOUNT_THRESHOLD", 0.40))