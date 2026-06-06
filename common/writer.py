from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from common.config import STAGING_FILE


def write_event_to_staging(
    event: dict[str, Any],
    staging_file=STAGING_FILE,
) -> None:
    """Append an accepted streaming event to the staging JSONL file."""
    staging_file.parent.mkdir(parents=True, exist_ok=True)

    # Add pipeline metadata before appending the accepted event to staging.
    staging_record = {
        **event,
        "dedup_status": "accepted",
        "ingested_at": datetime.now(UTC).isoformat(),
    }

    with staging_file.open("a", encoding="utf-8") as file:
        file.write(json.dumps(staging_record, ensure_ascii=False) + "\n")