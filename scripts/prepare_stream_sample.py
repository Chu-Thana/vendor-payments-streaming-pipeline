import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from common.config import (  # noqa: E402
    PROJECT1_SILVER_SAMPLE_FILE,
    RANDOM_SEED,
    STREAM_SAMPLE_FILE,
    STREAM_SAMPLE_SIZE,
)

def prepare_stream_sample() -> None:
    """Create a streaming input sample from Project 1 silver output."""

    if not PROJECT1_SILVER_SAMPLE_FILE.exists():
        raise FileNotFoundError(
            f"Project 1 silver sample file not found: {PROJECT1_SILVER_SAMPLE_FILE}"
        )

    STREAM_SAMPLE_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Read cleaned Project 1 silver output as the source for streaming events.
    df = pd.read_csv(PROJECT1_SILVER_SAMPLE_FILE)

    if df.empty:
        raise ValueError("Project 1 silver sample file is empty.")

    sample_size = min(STREAM_SAMPLE_SIZE, len(df))

    stream_df = df.sample(
        n=sample_size,
        random_state=RANDOM_SEED,
        replace=False,
    ).copy()

    now = datetime.now(UTC).isoformat()

    # Add event metadata required by the Kafka producer and downstream consumers.
    stream_df.insert(0, "event_id", [str(uuid.uuid4()) for _ in range(len(stream_df))])
    stream_df.insert(1, "event_type", "vendor_payment_event")
    stream_df.insert(2, "event_timestamp", now)
    stream_df.insert(3, "source_system", "vendor_payments_project1_silver")

    stream_df.to_csv(STREAM_SAMPLE_FILE, index=False)

    print(f"Created streaming sample: {STREAM_SAMPLE_FILE}")
    print(f"Rows: {len(stream_df):,}")
    print(f"Source: {PROJECT1_SILVER_SAMPLE_FILE}")


if __name__ == "__main__":
    prepare_stream_sample()