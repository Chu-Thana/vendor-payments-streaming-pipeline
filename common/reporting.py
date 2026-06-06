from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common.config import (
    STAGING_FILE,
    STREAMING_SUMMARY_REPORT_FILE,
    TOPIC_VENDOR_PAYMENTS,
)


def calculate_duplicate_rate(
    produced_events: int,
    rejected_duplicates: int,
) -> float:
    """Calculate duplicate rate from produced and rejected duplicate events."""
    if produced_events == 0:
        return 0.0

    return rejected_duplicates / produced_events


def build_streaming_summary_report(
    produced_events: int,
    accepted_events: int,
    rejected_duplicates: int,
    base_events: int | None = None,
    topic: str = TOPIC_VENDOR_PAYMENTS,
    staging_file: Path = STAGING_FILE,
) -> dict[str, Any]:
    """Build a summary report for the streaming pipeline run."""
    duplicate_rate = calculate_duplicate_rate(
        produced_events=produced_events,
        rejected_duplicates=rejected_duplicates,
    )

    return {
        "report_generated_at": datetime.now(UTC).isoformat(),
        "topic": topic,
        "base_events": base_events,
        "produced_events": produced_events,
        "accepted_events": accepted_events,
        "rejected_duplicates": rejected_duplicates,
        "duplicate_rate": round(duplicate_rate, 4),
        "dedup_strategy": "redis_event_id_first_level_dedup",
        "staging_output": str(staging_file),
        "principle": "Prevent data loss first, then handle duplicates safely.",
    }


def write_streaming_summary_report(
    report: dict[str, Any],
    report_file: Path = STREAMING_SUMMARY_REPORT_FILE,
) -> None:
    """Write streaming summary report to JSON file."""
    report_file.parent.mkdir(parents=True, exist_ok=True)

    report_file.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )