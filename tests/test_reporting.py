import json

from common.reporting import (
    build_streaming_summary_report,
    calculate_duplicate_rate,
    write_streaming_summary_report,
)


def test_calculate_duplicate_rate():
    duplicate_rate = calculate_duplicate_rate(
        produced_events=105000,
        rejected_duplicates=5000,
    )

    assert round(duplicate_rate, 4) == 0.0476


def test_calculate_duplicate_rate_returns_zero_when_no_events():
    duplicate_rate = calculate_duplicate_rate(
        produced_events=0,
        rejected_duplicates=0,
    )

    assert duplicate_rate == 0.0


def test_build_streaming_summary_report_contains_expected_metrics():
    report = build_streaming_summary_report(
        base_events=100000,
        produced_events=105000,
        accepted_events=100000,
        rejected_duplicates=5000,
    )

    assert report["base_events"] == 100000
    assert report["produced_events"] == 105000
    assert report["accepted_events"] == 100000
    assert report["rejected_duplicates"] == 5000
    assert report["duplicate_rate"] == 0.0476
    assert report["dedup_strategy"] == "redis_event_id_first_level_dedup"
    assert (
        report["principle"]
        == "Prevent data loss first, then handle duplicates safely."
    )


def test_write_streaming_summary_report_creates_json_file(tmp_path):
    report_file = tmp_path / "streaming_summary_report.json"

    report = build_streaming_summary_report(
        base_events=100000,
        produced_events=105000,
        accepted_events=100000,
        rejected_duplicates=5000,
    )

    write_streaming_summary_report(report, report_file=report_file)

    saved_report = json.loads(report_file.read_text(encoding="utf-8"))

    assert saved_report["produced_events"] == 105000
    assert saved_report["accepted_events"] == 100000
    assert saved_report["rejected_duplicates"] == 5000