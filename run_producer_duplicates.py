"""
Entry point for running the duplicate-event Kafka producer.

Why this file exists
--------------------
This project keeps producer logic inside the producer/ package
for cleaner organization and easier maintenance.

Instead of running:

    python -m producer.producer_with_duplicates

we provide a simpler entry point:

    python run_producer_duplicates.py

This makes duplicate-event testing easier for developers and reviewers.
"""

# Import the duplicate-event producer entry point
from producer.producer_with_duplicates import main


def run():
    """
    Start the duplicate-event producer.
    """
    main()


# Standard Python entry point
if __name__ == "__main__":
    run()