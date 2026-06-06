import json

from common.writer import write_event_to_staging


def test_write_event_to_staging_creates_jsonl_record(tmp_path):
    staging_file = tmp_path / "staging.jsonl"

    event = {
        "event_id": "event-001",
        "event_type": "vendor_payment_event",
        "event_timestamp": "2026-06-06T00:00:00+00:00",
        "source_system": "vendor_payments_project1_silver",
        "source_row_hash": "hash-001",
        "business_composite_key": "key-001",
        "payment_amount": 12500.50,
    }

    write_event_to_staging(event, staging_file=staging_file)

    lines = staging_file.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1

    record = json.loads(lines[0])

    assert record["event_id"] == "event-001"
    assert record["dedup_status"] == "accepted"
    assert "ingested_at" in record