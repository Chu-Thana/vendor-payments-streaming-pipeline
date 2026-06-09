from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import redis

from common.config import DEDUP_TTL_SECONDS, REDIS_HOST, REDIS_PORT


class RedisDeduplicator:
    """First-level Redis deduplication for streaming events."""

    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        ttl_seconds: int = DEDUP_TTL_SECONDS,
    ) -> None:
        self.client = redis.Redis(
            host=host,
            port=port,
            decode_responses=True,
        )
        self.ttl_seconds = ttl_seconds

    @staticmethod
    def build_dedup_key(event_id: str) -> str:
        """Build Redis deduplication key from event_id."""
        return f"event:{event_id}"

    def is_duplicate(self, event_id: str) -> bool:
        """Check whether event_id already exists in Redis."""
        key = self.build_dedup_key(event_id)
        return self.client.exists(key) == 1

    def mark_processed(self, event: dict[str, Any]) -> None:
        """Store event_id in Redis after successful first-level acceptance."""
        event_id = str(event["event_id"])
        key = self.build_dedup_key(event_id)

        value = {
            "event_id": event_id,
            "source_row_hash": event.get("source_row_hash"),
            "business_composite_key": event.get("business_composite_key"),
            "first_seen_at": datetime.now(UTC).isoformat(),
        }

        self.client.setex(
            name=key,
            time=self.ttl_seconds,
            value=json.dumps(value, ensure_ascii=False),
        )

    def should_accept(self, event: dict[str, Any]) -> bool:
        """Legacy helper. Do not use when durable staging write must happen before Redis mark."""
        event_id = str(event["event_id"])

        if self.is_duplicate(event_id):
            return False

        self.mark_processed(event)
        return True