from framework.notifications.notify import (
    make_notification,
    notification_create_sql,
    notification_table_name,
    notification_missing_columns,
    resolve_recipients,
)
from framework.notifications.constants import (
    SEVERITY_INFO, SEVERITY_WARNING, SEVERITY_ERROR,
    CAT_DISPATCHER, CAT_INGESTION, CAT_CONFIG, CAT_SCHEMA, CAT_PARSE,
    EVT_DUPLICATE_CONFIG_RESOLVED, EVT_NO_ELIGIBLE_FILES, EVT_SCHEMA_DRIFT_DETECTED,
)


def test_make_notification_basic():
    n = make_notification(
        SEVERITY_WARNING, CAT_CONFIG, EVT_DUPLICATE_CONFIG_RESOLVED,
        "Duplicate resolved",
        feed_key="feed_a",
        dispatch_run_id="abc123",
    )
    assert n["severity"] == "WARNING"
    assert n["category"] == "CONFIG"
    assert n["event_type"] == "DUPLICATE_CONFIG_RESOLVED"
    assert n["message"] == "Duplicate resolved"
    assert n["feed_key"] == "feed_a"
    assert n["dispatch_run_id"] == "abc123"
    assert n["request_id"] is None
    assert n["details_json"] is None
    assert n["resolved_recipients"] is None
    assert len(n["event_id"]) == 32  # UUID hex


def test_make_notification_with_details():
    n = make_notification(
        SEVERITY_INFO, CAT_INGESTION, EVT_NO_ELIGIBLE_FILES,
        "No files",
        details={"count": 0, "reason": "empty source"},
    )
    assert n["details_json"] is not None
    import json
    parsed = json.loads(n["details_json"])
    assert parsed["count"] == 0
    assert parsed["reason"] == "empty source"


def test_make_notification_case_insensitive():
    n = make_notification("warning", "schema", EVT_SCHEMA_DRIFT_DETECTED, "drift")
    assert n["severity"] == "WARNING"
    assert n["category"] == "SCHEMA"


def test_make_notification_invalid_severity():
    try:
        make_notification("CRITICAL", CAT_CONFIG, "EVT", "msg")
        assert False, "expected ValueError"
    except ValueError as e:
        assert "severity" in str(e).lower()


def test_make_notification_invalid_category():
    try:
        make_notification(SEVERITY_INFO, "UNKNOWN", "EVT", "msg")
        assert False, "expected ValueError"
    except ValueError as e:
        assert "category" in str(e).lower()


def test_make_notification_with_resolved_recipients():
    n = make_notification(
        SEVERITY_WARNING, CAT_CONFIG, EVT_DUPLICATE_CONFIG_RESOLVED,
        "test",
        resolved_recipients="team-aca-dl@aetna.com",
    )
    assert n["resolved_recipients"] == "team-aca-dl@aetna.com"


def test_make_notification_resolved_recipients_default_none():
    n = make_notification(SEVERITY_INFO, CAT_INGESTION, EVT_NO_ELIGIBLE_FILES, "test")
    assert n["resolved_recipients"] is None


def test_notification_table_name():
    assert notification_table_name("cat", "schema") == "cat.schema.ops_notifications"


def test_notification_create_sql():
    sql = notification_create_sql("cat.schema.ops_notifications")
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "severity" in sql
    assert "details_json" in sql
    assert "resolved_recipients" in sql


def test_notification_missing_columns_all_present():
    cols = ["event_id", "severity", "category", "event_type", "feed_key",
            "dispatch_run_id", "request_id", "message", "details_json",
            "resolved_recipients", "ts_event"]
    assert notification_missing_columns(cols) == []


def test_notification_missing_columns_partial():
    cols = ["event_id", "severity"]
    missing = notification_missing_columns(cols)
    assert len(missing) == 9
    missing_names = [n for n, _ in missing]
    assert "category" in missing_names
    assert "details_json" in missing_names
    assert "resolved_recipients" in missing_names


def test_resolve_recipients_override_wins():
    result = resolve_recipients("team-aca-dl@aetna.com", "roderick.fernan2@aetna.com")
    assert result == "roderick.fernan2@aetna.com"


def test_resolve_recipients_feed_fallback():
    result = resolve_recipients("team-aca-dl@aetna.com", None)
    assert result == "team-aca-dl@aetna.com"


def test_resolve_recipients_both_none():
    result = resolve_recipients(None, None)
    assert result is None


def test_resolve_recipients_empty_strings():
    result = resolve_recipients("", "")
    assert result is None


def test_resolve_recipients_whitespace_override():
    result = resolve_recipients("team@aetna.com", "  ")
    assert result == "team@aetna.com"
