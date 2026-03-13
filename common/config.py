# Shared configuration for the local Kafka streaming project.

KAFKA_BROKER = "localhost:9092"
TOPIC_SALES = "sales_events"

# Business rule for alerting
HIGH_VALUE_THRESHOLD = 400

# Redis configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Redis deduplication window (seconds)
DEDUP_TTL_SECONDS = 3600

# Logging configuration
LOG_LEVEL = "INFO"
KAFKA_LOG_LEVEL = "WARNING"
REDIS_LOG_LEVEL = "WARNING"