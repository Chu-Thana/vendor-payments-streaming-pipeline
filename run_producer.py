"""
Entry point for running the standard Kafka producer.

Why this file exists
--------------------
The project is organized into packages such as:

    producer/
    consumer/
    common/

This structure keeps the code modular and clean, but it also means
running internal files directly can be inconvenient.

Instead of asking users to run:

    python -m producer.producer

we provide a simple entry point:

    python run_producer.py

This improves developer experience while preserving a clean package structure.
"""

# Import the main producer function from the producer module
from producer.producer import main


def run():
    """
    Start the standard event producer.
    """
    main()


# Standard Python entry point
if __name__ == "__main__":
    run()