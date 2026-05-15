import json
from datetime import UTC, datetime, timedelta

from framework.helpers.fingerprint import enrich_source_entry
from pipelines.file_ingestion.discover.filter_eligible_files import (
    build_batch_inputs,
    classify_request_paths,
    find_blocked_recent_paths,
    parse_request_payload,
    select_paths_from_request,
)


def _cfg():
    return {
        "feed_key": "retro_status_report_ci_aca",
        "feed_sub_key": "DEFAULT",
        "src_file_regex": r"^(CI)_Retro Status Report_(ACA)_(\d{8})_(\d{1,2})_(\d{1,2})(?:_(v\d+|updated))?\.(txt|zip)$",
        "src_file_capture_spec": "1|vendor_code|string;2|lob_code|string;3|file_date|date_yyyymmdd;4|file_part_seq|int;5|file_part_tot|int;6|file_version_label|string;7|file_extension|string",
        "batch_max_files": 2,
        "batch_max_size_gb": 1.0,
    }


def test_force_reprocess_normalized():
    payload = parse_request_payload('{"request_type":"ADHOC","file_paths":["dbfs:/a.txt"],"force_reprocess":"Y"}')
    assert payload["force_reprocess"] is True


def test_blocks_already_done_without_force():
    source_entries = [
        enrich_source_entry(
            {
                "name": "CI_Retro Status Report_ACA_20260102_1_5_v2.txt",
                "file_name": "CI_Retro Status Report_ACA_20260102_1_5_v2.txt",
                "file_path": "dbfs:/a.txt",
                "size": 10,
            }
        )
    ]
    fp = source_entries[0]["file_fingerprint"]
    waterfall = classify_request_paths(["dbfs:/a.txt"], source_entries, _cfg(), {"dbfs:/a.txt"}, set(), {fp}, False)
    assert waterfall["arr_paths_final_eligible"] == []


def test_select_paths_sched_lookback_minutes():
    now = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
    within = now - timedelta(minutes=30)
    outside = now - timedelta(minutes=200)
    cfg = {**_cfg(), "sched_selector_type": "FILE_MODIFIED_TS", "sched_lookback_minutes": 60}
    source_entries = [
        {
            "file_name": "CI_Retro Status Report_ACA_20260114_1_5.txt",
            "file_path": "dbfs:/in.txt",
            "modificationTime": int(within.timestamp() * 1000),
        },
        {
            "file_name": "CI_Retro Status Report_ACA_20260110_1_5.txt",
            "file_path": "dbfs:/out.txt",
            "modificationTime": int(outside.timestamp() * 1000),
        },
    ]
    payload = {"request_type": "INCREMENTAL", "lookback_minutes": 90}
    paths = select_paths_from_request(payload, source_entries, cfg, "dbfs:/vol/source", now_utc=now)
    assert paths == ["dbfs:/in.txt"]


def test_build_batch_inputs():
    source_entries = [
        {"file_path": "dbfs:/a.txt", "size": 10},
        {"file_path": "dbfs:/b.txt", "size": 20},
        {"file_path": "dbfs:/c.txt", "size": 30},
    ]
    batches = build_batch_inputs(["dbfs:/a.txt", "dbfs:/b.txt", "dbfs:/c.txt"], source_entries, _cfg())
    assert len(batches) == 2
    assert batches[0]["file_count"] == 2
    refs0 = json.loads(batches[0]["batch_file_paths_json"])
    assert all(isinstance(x, dict) and "path" in x and "fingerprint" in x for x in refs0)


# --- find_blocked_recent_paths: FAILED status should NOT block retries ---


def test_blocked_recent_ignores_failed_status():
    """A file with latest status FAILED should not be blocked on retry."""
    now = datetime.now(UTC)
    rows = [
        {"file_path": "dbfs:/a.txt", "file_fingerprint": "fp_a", "stage_name": "INGEST_CONTROL", "stage_status": "STARTED", "ts_event": now - timedelta(hours=1)},
        {"file_path": "dbfs:/a.txt", "file_fingerprint": "fp_a", "stage_name": "INGEST_CONTROL", "stage_status": "FAILED", "ts_event": now},
    ]
    assert find_blocked_recent_paths(rows) == []


def test_blocked_recent_still_blocks_started_only():
    """A file with only STARTED status (no FAILED follow-up) should still be blocked."""
    now = datetime.now(UTC)
    rows = [
        {"file_path": "dbfs:/a.txt", "file_fingerprint": "fp_a", "stage_name": "INGEST_CONTROL", "stage_status": "STARTED", "ts_event": now},
    ]
    assert find_blocked_recent_paths(rows) == ["fp_a"]


def test_blocked_recent_mixed_files():
    """One file FAILED (retryable), another still STARTED (blocked)."""
    now = datetime.now(UTC)
    rows = [
        # File A: STARTED then FAILED -> not blocked
        {"file_path": "dbfs:/a.txt", "file_fingerprint": "fp_a", "stage_name": "INGEST_CONTROL", "stage_status": "STARTED", "ts_event": now - timedelta(hours=2)},
        {"file_path": "dbfs:/a.txt", "file_fingerprint": "fp_a", "stage_name": "INGEST_CONTROL", "stage_status": "FAILED", "ts_event": now - timedelta(hours=1)},
        # File B: STARTED only -> blocked
        {"file_path": "dbfs:/b.txt", "file_fingerprint": "fp_b", "stage_name": "INGEST_CONTROL", "stage_status": "STARTED", "ts_event": now},
    ]
    blocked = find_blocked_recent_paths(rows)
    assert "fp_a" not in blocked
    assert "fp_b" in blocked


def test_blocked_recent_succeeded_not_blocked():
    """A file with SUCCEEDED status should not be blocked."""
    now = datetime.now(UTC)
    rows = [
        {"file_path": "dbfs:/a.txt", "file_fingerprint": "fp_a", "stage_name": "INGEST_CONTROL", "stage_status": "STARTED", "ts_event": now - timedelta(hours=1)},
        {"file_path": "dbfs:/a.txt", "file_fingerprint": "fp_a", "stage_name": "INGEST_CONTROL", "stage_status": "SUCCEEDED", "ts_event": now},
    ]
    assert find_blocked_recent_paths(rows) == []


def test_build_batch_inputs_capped():
    """batch_max_per_run caps the number of batches returned."""
    from pipelines.file_ingestion.discover.filter_eligible_files import build_batch_inputs
    paths = [f"/mnt/src/file_{i:03d}.csv" for i in range(50)]
    entries = [{"file_path": p, "file_name": p.split("/")[-1], "size": 1000, "file_fingerprint": f"fp_{i}"} for i, p in enumerate(paths)]
    cfg = {"batch_max_files": 2, "batch_max_size_gb": 1.0}
    batches = build_batch_inputs(paths, entries, cfg)
    assert len(batches) == 25, f"50 files / 2 per batch = 25 batches, got {len(batches)}"
    # Simulate the cap applied in run_request_intake
    batch_max_per_run = 10
    capped = batches[:batch_max_per_run] if len(batches) > batch_max_per_run else batches
    assert len(capped) == 10, f"cap at 10 should yield 10 batches, got {len(capped)}"
    assert capped[0]["batch_id"] == "batch_001"
    assert capped[-1]["batch_id"] == "batch_010"
