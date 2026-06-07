"""
Entry point for running the Vendor Payments Kafka consumer.

Usage:
    python run_consumer.py
    python run_consumer.py consumer-A

The consumer reads Vendor Payments events from Kafka, applies first-level
Redis deduplication, writes accepted events to staging, and generates a
streaming summary report for downstream validation.
"""

import sys

from consumer.consumer import main


if __name__ == "__main__":
    consumer_name = sys.argv[1] if len(sys.argv) > 1 else "consumer-A"
    main(consumer_name)