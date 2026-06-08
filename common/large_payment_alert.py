from __future__ import annotations

from typing import Any

from common.config import LARGE_PAYMENT_THRESHOLD


def get_payment_amount(event: dict[str, Any]) -> float:
    """Extract payment amount from a vendor payment event."""
    amount = event.get("vouchers_paid", event.get("payment_amount", 0))

    if amount is None:
        return 0.0

    return float(amount)


def is_large_payment_event(
    event: dict[str, Any],
    threshold: float = LARGE_PAYMENT_THRESHOLD,
) -> bool:
    """Return True when the event amount exceeds the large payment threshold."""
    return abs(get_payment_amount(event)) >= threshold


def build_large_payment_alert_message(event: dict[str, Any]) -> str:
    """Build Telegram message for a large vendor payment event."""
    amount = get_payment_amount(event)
    supplier_name = event.get("supplier_name") or event.get("vendor_name")
    department = event.get("department") or event.get("department_name")

    return (
        "🚨 LARGE VENDOR PAYMENT ALERT\n\n"
        f"event_id: {event.get('event_id')}\n"
        f"supplier: {supplier_name}\n"
        f"department: {department}\n"
        f"fiscal_year: {event.get('fiscal_year')}\n"
        f"vouchers_paid: {amount:,.2f}\n"
        f"source: {event.get('source_system')}\n"
    )