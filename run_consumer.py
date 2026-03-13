import sys

from common.logging_config import setup_logging
from consumer.consumer import main


# ==================================
# Logging Bootstrap
# ==================================
# Configure logging before starting the consumer.
#
# This ensures:
# - application logs are readable
# - Kafka library logs are reduced
# - structured fields from logger.extra are rendered properly
setup_logging()


# ==================================
# Script Entry Point
# ==================================
# Usage:
#   python run_consumer.py consumer-A
#   python run_consumer.py consumer-B
#
# If no name is provided, default to consumer-A.
if __name__ == "__main__":
    consumer_name = sys.argv[1] if len(sys.argv) > 1 else "consumer-A"
    main(consumer_name)