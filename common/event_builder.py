from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd


REQUIRED_EVENT_FIELDS = [
    "event_id",
    "event_type",
    "event_timestamp",
    "source_system",
]


def _to_json_safe_value(value: Any) -> Any:
    """Convert pandas/numpy values into JSON-safe Python values."""
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    return value


def build_vendor_payment_event(row: pd.Series) -> dict[str, Any]:
    """Build a vendor payment event from a streaming sample row."""
    event = {
        "event_id": str(row["event_id"]),
        "event_type": str(row.get("event_type", "vendor_payment_event")),
        "event_timestamp": str(
            row.get("event_timestamp", datetime.now(UTC).isoformat())
        ),

        # Promote frequently used fields to the top level for routing, deduplication, and alerting.
        "source_system": str(
            row.get("source_system", "vendor_payments_project1_silver")
        ),
        "source_row_hash": _to_json_safe_value(row.get("source_row_hash")),
        "business_composite_key": _to_json_safe_value(
            row.get("business_composite_key")
        ),
        "fiscal_year": _to_json_safe_value(row.get("fiscal_year")),
        "vendor_name": _to_json_safe_value(row.get("vendor_name")),
        "vouchers_paid": _to_json_safe_value(row.get("vouchers_paid")),
        "department_name": _to_json_safe_value(row.get("department_name")),
        "payment_status": _to_json_safe_value(row.get("payment_status")),
        "payment_amount": _to_json_safe_value(row.get("payment_amount")),

        # Keep the full original row as payload so downstream consumers do not lose source details.
        "payload": {
            key: _to_json_safe_value(value)
            for key, value in row.to_dict().items()
        },
    }

    missing_fields = [
        field for field in REQUIRED_EVENT_FIELDS if not event.get(field)
    ]

    if missing_fields:
        raise ValueError(f"Missing required event fields: {missing_fields}")

    return event