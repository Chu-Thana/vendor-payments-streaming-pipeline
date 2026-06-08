import pytest

from consumer.consumer import validate_event

# Unit tests for validating required Kafka event metadata before consumer processing.

def test_validate_event_accepts_required_fields():
    event = {
        "event_id": "event-001",
        "event_type": "vendor_payment_event",
        "event_timestamp": "2026-06-06T00:00:00+00:00",
        "source_system": "vendor_payments_project1_silver",
    }

    validate_event(event)


def test_validate_event_rejects_missing_event_id():
    event = {
        "event_type": "vendor_payment_event",
        "event_timestamp": "2026-06-06T00:00:00+00:00",
        "source_system": "vendor_payments_project1_silver",
    }

    with pytest.raises(ValueError, match="Missing required event fields"):
        validate_event(event)