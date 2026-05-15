"""Record builder functions for all tracking tables."""
from __future__ import annotations

import json
from datetime import datetime, UTC
from uuid import uuid4


def make_job_log_record(
    task_name: str,
    task_status: str,
    feed_key: str,
    request_id: str | None = None,
    status_reason: str | None = None,
    *,
    dispatch_run_id: str | None = None,
) -> dict:
    return {
        "event_id": uuid4().hex,
        "request_id": request_id,
        "task_name": task_name,
        "task_status": task_status,
        "feed_key": feed_key,
        "status_reason": status_reason,
        "dispatch_run_id": dispatch_run_id,
        "ts_event": datetime.now(UTC),
    }


def make_file_log_record(
    file_path: str,
    file_name: str,
    feed_key: str,
    stage_name: str,
    stage_status: str,
    vendor_code=None, lob_code=None, file_date=None,
    file_part_seq=None, file_part_tot=None,
    file_version_label=None, file_version_rank=None, file_extension=None,
    status_reason=None, cnt_row_written=None, request_id=None,
    file_fingerprint: str | None = None,
    *,
    dispatch_run_id: str | None = None,
) -> dict:
    return {
        "event_id": uuid4().hex,
        "request_id": request_id,
        "file_path": file_path,
        "file_fingerprint": file_fingerprint or "",
        "file_name": file_name,
        "feed_key": feed_key,
        "stage_name": stage_name,
        "stage_status": stage_status,
        "vendor_code": vendor_code,
        "lob_code": lob_code,
        "file_date": file_date,
        "file_part_seq": file_part_seq,
        "file_part_tot": file_part_tot,
        "file_version_label": file_version_label,
        "file_version_rank": file_version_rank,
        "file_extension": file_extension,
        "status_reason": status_reason,
        "cnt_row_written": cnt_row_written,
        "dispatch_run_id": dispatch_run_id,
        "ts_event": datetime.now(UTC),
    }


def make_schema_change_record(
    file_path: str,
    file_name: str,
    feed_key: str,
    target_table: str,
    source_columns: list[str],
    target_columns: list[str],
    missing_in_file: list[str],
    new_in_file: list[str],
    change_detected: str,
    status_reason: str,
    request_id: str | None = None,
    file_fingerprint: str | None = None,
    *,
    dispatch_run_id: str | None = None,
) -> dict:
    return {
        "event_id": uuid4().hex,
        "request_id": request_id,
        "file_path": file_path,
        "file_fingerprint": file_fingerprint or "",
        "file_name": file_name,
        "feed_key": feed_key,
        "target_table": target_table,
        "source_columns_csv": ",".join(source_columns),
        "target_columns_csv": ",".join(target_columns),
        "missing_in_file": missing_in_file,
        "new_in_file": new_in_file,
        "change_detected": change_detected,
        "status_reason": status_reason,
        "dispatch_run_id": dispatch_run_id,
        "ts_event": datetime.now(UTC),
    }


def get_task_value(dbutils, task_key: str, key: str, default=None):
    if default is None:
        default = ""
    try:
        value = dbutils.jobs.taskValues.get(taskKey=task_key, key=key, debugValue=default)
    except Exception:
        return default
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") or stripped.startswith("{"):
            try:
                return json.loads(stripped)
            except Exception:
                return value
    return value
