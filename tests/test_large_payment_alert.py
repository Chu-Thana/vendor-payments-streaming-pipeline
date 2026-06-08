from common.large_payment_alert import (
    build_large_payment_alert_message,
    get_payment_amount,
    is_large_payment_event,
)


def test_get_payment_amount_uses_vouchers_paid():
    event = {"vouchers_paid": 1_500_000}

    assert get_payment_amount(event) == 1_500_000


def test_is_large_payment_event_returns_true_when_amount_exceeds_threshold():
    event = {"vouchers_paid": 1_500_000}

    assert is_large_payment_event(event, threshold=1_000_000)


def test_is_large_payment_event_returns_false_when_amount_below_threshold():
    event = {"vouchers_paid": 500_000}

    assert not is_large_payment_event(event, threshold=1_000_000)


def test_build_large_payment_alert_message_contains_expected_fields():
    event = {
        "event_id": "event-001",
        "supplier_name": "ABC SUPPLIER",
        "department": "FINANCE",
        "fiscal_year": 2026,
        "vouchers_paid": 1_500_000,
        "source_system": "vendor_payments_project1_silver",
    }

    message = build_large_payment_alert_message(event)

    assert "LARGE VENDOR PAYMENT ALERT" in message
    assert "event-001" in message
    assert "ABC SUPPLIER" in message
    assert "1,500,000.00" in message