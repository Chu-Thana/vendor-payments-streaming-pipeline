"""
Entry point for running the Vendor Payments Kafka producer.

This wrapper keeps the command simple:

    python run_producer.py

The producer reads the prepared Vendor Payments streaming sample,
builds Kafka events, intentionally injects duplicate events to simulate
retry/replay scenarios, and publishes them to the vendor payments Kafka topic.
"""

from producer.producer import main


def run() -> None:
    """Start the Vendor Payments event producer."""
    main()


if __name__ == "__main__":
    run()