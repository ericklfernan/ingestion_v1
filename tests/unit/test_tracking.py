from framework.tracking.ddl import job_log_missing_columns, missing_dispatch_run_id_only
from framework.tracking.records import make_file_log_record, make_job_log_record


def test_job_log_missing_columns_adds_dispatch_run_id_when_absent():
    missing = job_log_missing_columns(
        ["event_id", "request_id", "task_name", "task_status", "feed_key", "status_reason", "ts_event"]
    )
    assert missing == [("dispatch_run_id", "STRING")]


def test_job_log_missing_columns_empty_when_present():
    assert job_log_missing_columns(["dispatch_run_id", "event_id"]) == []


def test_missing_dispatch_run_id_only():
    assert missing_dispatch_run_id_only(["event_id"]) == [("dispatch_run_id", "STRING")]
    assert missing_dispatch_run_id_only(["Dispatch_Run_Id"]) == []


def test_make_file_log_record_dispatch_run_id():
    r = make_file_log_record("p", "f.txt", "fk", "ST", "OK", request_id="rid")
    assert r["dispatch_run_id"] is None
    assert r["file_fingerprint"] == ""
    r2 = make_file_log_record("p", "f.txt", "fk", "ST", "OK", request_id="rid", file_fingerprint="ab", dispatch_run_id="abc")
    assert r2["dispatch_run_id"] == "abc"
    assert r2["file_fingerprint"] == "ab"


def test_make_job_log_record_dispatch_run_id_optional():
    r = make_job_log_record("dispatcher", "SUCCEEDED", "some_key", None, "note")
    assert r["dispatch_run_id"] is None
    assert r["task_name"] == "dispatcher"
    r2 = make_job_log_record("dispatcher", "SUCCEEDED", "some_key", None, "note", dispatch_run_id="abc123")
    assert r2["dispatch_run_id"] == "abc123"
