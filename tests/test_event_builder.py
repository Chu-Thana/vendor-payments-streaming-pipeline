import pandas as pd
import pytest

from common.event_builder import build_vendor_payment_event

# Unit tests for converting streaming sample rows into Kafka-ready vendor payment events.

def test_build_vendor_payment_event_contains_required_fields():
    row = pd.Series(
        {
            "event_id": "event-001",
            "event_type": "vendor_payment_event",
            "event_timestamp": "2026-06-06T00:00:00+00:00",
            "source_system": "vendor_payments_project1_silver",
            "source_row_hash": "hash-001",
            "business_composite_key": "key-001",
            "fiscal_year": 2025,
            "vendor_name": "ABC SUPPLIER",
            "department_name": "FINANCE",
            "payment_status": "PAID",
            "payment_amount": 12500.50,
        }
    )

    event = build_vendor_payment_event(row)

    assert event["event_id"] == "event-001"
    assert event["event_type"] == "vendor_payment_event"
    assert event["source_row_hash"] == "hash-001"
    assert event["business_composite_key"] == "key-001"
    assert event["payload"]["vendor_name"] == "ABC SUPPLIER"


def test_build_vendor_payment_event_rejects_missing_event_id():
    row = pd.Series(
        {
            "event_id": "",
            "event_type": "vendor_payment_event",
            "event_timestamp": "2026-06-06T00:00:00+00:00",
            "source_system": "vendor_payments_project1_silver",
        }
    )

    with pytest.raises(ValueError, match="Missing required event fields"):
        build_vendor_payment_event(row)