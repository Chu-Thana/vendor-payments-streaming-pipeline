import os
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()

# Central configuration for the Vendor Payments streaming pipeline.
# Values are loaded from environment variables when available,
# with local development defaults provided for reproducibility.

# ==================================
# Base paths
# ==================================
BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
EVENT_DIR = OUTPUT_DIR / "event"
STAGING_DIR = OUTPUT_DIR / "staging"
METRIC_DIR = OUTPUT_DIR / "metric"
REPORT_DIR = OUTPUT_DIR / "reports"
ALERT_DIR = BASE_DIR / "alerts"

# ==================================
# Vendor Payments input / staging
# ==================================
PROJECT1_ROOT = Path(
    os.getenv(
        "PROJECT1_ROOT",
        r"E:\dev\vendor-payments-etl-analytics",
    )
)

PROJECT1_SILVER_SAMPLE_FILE = Path(
    os.getenv(
        "PROJECT1_SILVER_SAMPLE_FILE",
        str(
            PROJECT1_ROOT
            / "data"
            / "processed"
            / "silver"
            / "vendor_payments_silver_sample.csv"
        ),
    )
)

STREAM_SAMPLE_FILE = Path(
    os.getenv(
        "STREAM_SAMPLE_FILE",
        str(INPUT_DIR / "vendor_payments_stream_sample.csv"),
    )
)

STAGING_FILE = Path(
    os.getenv(
        "STAGING_FILE",
        str(STAGING_DIR / "vendor_payments_streaming_staging.jsonl"),
    )
)

STREAMING_SUMMARY_REPORT_FILE = Path(
    os.getenv(
        "STREAMING_SUMMARY_REPORT_FILE",
        str(REPORT_DIR / "streaming_summary_report.json"),
    )
)

STREAM_SAMPLE_SIZE = int(os.getenv("STREAM_SAMPLE_SIZE", "100000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.05"))
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))

# ==================================
# Kafka
# ==================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")

TOPIC_VENDOR_PAYMENTS = os.getenv(
    "TOPIC_VENDOR_PAYMENTS",
    "vendor_payments_events",
)

TOPIC_DUPLICATE_VENDOR_PAYMENTS = os.getenv(
    "TOPIC_DUPLICATE_VENDOR_PAYMENTS",
    "vendor_payments_duplicate_events",
)

TOPIC_VENDOR_PAYMENT_ALERTS = os.getenv(
    "TOPIC_VENDOR_PAYMENT_ALERTS",
    "vendor_payment_alerts",
)

# ==================================
# Redis
# ==================================
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Redis first-level deduplication window in seconds.
DEDUP_TTL_SECONDS = int(os.getenv("DEDUP_TTL_SECONDS", "86400"))

# ==================================
# Business rule thresholds
# ==================================
LARGE_PAYMENT_THRESHOLD = float(
    os.getenv("LARGE_PAYMENT_THRESHOLD", "1000000")
)

TELEGRAM_LARGE_PAYMENT_ALERT_LIMIT = int(
    os.getenv("TELEGRAM_LARGE_PAYMENT_ALERT_LIMIT", "5")
)

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