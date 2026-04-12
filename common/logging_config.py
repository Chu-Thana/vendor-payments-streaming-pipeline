import logging

from common.config import LOG_LEVEL, KAFKA_LOG_LEVEL, REDIS_LOG_LEVEL


# ==================================
# Custom Formatter
# ==================================
# Render logs in a compact key=value style.
#
# Example output:
# 2026-03-13 18:47:52 | WARNING | duplicate skipped
# | event=duplicate_skipped
# | consumer=consumer-B
# | store=redis
# | order_id=1001
#
# This format is:
# - easy to read in the terminal
# - consistent across events
# - suitable for portfolio/demo use
class KeyValueFormatter(logging.Formatter):
    """
    Format log records using a readable key=value layout.

    The formatter keeps the normal log message, then appends any known
    structured fields passed through logger.extra.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Build the final rendered log line.

        Output structure:
        - timestamp
        - level
        - message
        - optional structured fields
        """
        base = f"{self.formatTime(record, self.datefmt)} | {record.levelname}"

        parts = [record.getMessage()]

        # Structured fields supported by the application.
        #
        # Tuple format:
        #   (attribute_name_on_record, rendered_label)
        optional_fields = [
            ("event_type", "event"),
            ("consumer_name", "consumer"),
            ("store", "store"),
            ("topic_name", "topic"),
            ("partition_id", "partition"),
            ("offset_value", "offset"),
            ("order_id", "order_id"),
            ("region_name", "region"),
            ("sales_value", "sales"),
        ]

        for attr_name, label in optional_fields:
            value = getattr(record, attr_name, None)
            if value is not None:
                parts.append(f"{label}={value}")

        return f"{base} | " + " | ".join(parts)


# ==================================
# Logging Setup Function
# ==================================
# Configure root logging for the project.
#
# Behavior:
# - Application logs follow LOG_LEVEL
# - Kafka library logs are reduced using KAFKA_LOG_LEVEL
# - Redis library logs are reduced using REDIS_LOG_LEVEL
#
# This keeps terminal output focused on business-level events such as:
# - consumer startup
# - processed events
# - duplicate skips
# - high-value alerts
def setup_logging() -> None:
    """
    Configure project-wide logging.

    This function should be called once at process startup,
    before the consumer or producer begins running.
    """

    # Create a stream handler for terminal output.
    handler = logging.StreamHandler()
    handler.setFormatter(
        KeyValueFormatter(datefmt="%Y-%m-%d %H:%M:%S")
    )

    # Configure the root logger.
    #
    # Clear existing handlers first so repeated runs do not duplicate logs.
    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    root.handlers.clear()
    root.addHandler(handler)

    # Reduce noisy logs from Kafka client internals.
    logging.getLogger("kafka").setLevel(
        getattr(logging, KAFKA_LOG_LEVEL.upper(), logging.WARNING)
    )

    # Reduce noisy logs from Redis client internals.
    logging.getLogger("redis").setLevel(
        getattr(logging, REDIS_LOG_LEVEL.upper(), logging.WARNING)
    )

def get_logger(name: str):
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger
