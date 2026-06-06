from common.dedup import RedisDeduplicator


def test_build_dedup_key():
    key = RedisDeduplicator.build_dedup_key("event-001")

    assert key == "event:event-001"