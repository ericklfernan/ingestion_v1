"""Request parsing, path selection, classification, and batch building."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, UTC
from pathlib import PurePosixPath

from framework.helpers.fingerprint import compute_file_fingerprint, source_mtime_ms, source_size
from framework.helpers.filename_parser import parse_filename_metadata


def build_default_request_payload() -> dict:
    return {"request_type": "INCREMENTAL", "force_reprocess": False}


def normalize_force_reprocess(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().upper() in {"Y", "YES", "TRUE", "1"}


def parse_request_payload(request_json: str) -> dict:
    if not request_json or not request_json.strip():
        return build_default_request_payload()
    payload = json.loads(request_json)
    if not isinstance(payload, dict):
        raise ValueError("request_json must be a JSON object")
    payload["request_type"] = str(payload.get("request_type", "INCREMENTAL")).upper()
    payload["force_reprocess"] = normalize_force_reprocess(payload.get("force_reprocess", False))
    return payload


def normalize_path(path_or_name: str, source_dir_dbfs: str) -> str:
    value = path_or_name.strip()
    if value.startswith("dbfs:/"):
        return value
    lower = value.lower()
    if lower.startswith(("s3://", "s3a://", "abfss://", "wasbs://", "gs://")):
        return value
    if value.startswith("/Volumes/"):
        return f"dbfs:{value}"
    if "/" not in value:
        return f"{source_dir_dbfs}/{value}"
    return value


def select_paths_from_request(payload: dict, source_entries: list[dict], cfg: dict, source_dir_dbfs: str, now_utc: datetime | None = None) -> list[str]:
    request_type = str(payload["request_type"]).upper()
    source_by_path = {str(x["file_path"]): x for x in source_entries}
    now_utc = now_utc or datetime.now(UTC)

    if request_type in {"ADHOC", "DISCOVERY"}:
        raw_paths = payload.get("file_paths") or ([payload["file_path"]] if payload.get("file_path") else [])
        return sorted({normalize_path(str(p), source_dir_dbfs) for p in raw_paths})

    if request_type == "BACKFILL":
        selector_type = str(payload.get("selector_type", "FILE_DATE")).upper()
        selected = []
        if selector_type == "FILE_DATE":
            file_date_from = payload.get("file_date_from")
            file_date_to = payload.get("file_date_to")
            if not file_date_from or not file_date_to:
                raise ValueError("BACKFILL with FILE_DATE requires file_date_from and file_date_to")
            dt_from = datetime.strptime(file_date_from, "%Y-%m-%d").date()
            dt_to = datetime.strptime(file_date_to, "%Y-%m-%d").date()
            for entry in source_entries:
                parsed = parse_filename_metadata(entry["file_name"], cfg)
                if parsed["parse_status"] == "PARSED" and parsed["file_date"] is not None and dt_from <= parsed["file_date"] <= dt_to:
                    selected.append(entry["file_path"])
        else:
            modified_from_ts = payload.get("modified_from_ts")
            modified_to_ts = payload.get("modified_to_ts")
            if not modified_from_ts or not modified_to_ts:
                raise ValueError("BACKFILL with FILE_MODIFIED_TS requires modified_from_ts and modified_to_ts")
            dt_from = datetime.fromisoformat(modified_from_ts.replace("Z", "+00:00"))
            dt_to = datetime.fromisoformat(modified_to_ts.replace("Z", "+00:00"))
            for entry in source_entries:
                mod = entry.get("modificationTime")
                if mod is not None:
                    mod_dt = datetime.fromtimestamp(mod / 1000, tz=UTC)
                    if dt_from <= mod_dt <= dt_to:
                        selected.append(entry["file_path"])
        return sorted(set(selected))

    if request_type == "INCREMENTAL":
        selector_type = str(payload.get("selector_type") or cfg.get("sched_selector_type", "FILE_MODIFIED_TS")).upper()
        lookback_minutes = int(payload.get("lookback_minutes") or cfg.get("sched_lookback_minutes", 1440))
        selected = []
        if selector_type == "FILE_DATE":
            dt_from = (now_utc - timedelta(minutes=lookback_minutes)).date()
            dt_to = now_utc.date()
            for entry in source_entries:
                parsed = parse_filename_metadata(entry["file_name"], cfg)
                if parsed["parse_status"] == "PARSED" and parsed["file_date"] is not None and dt_from <= parsed["file_date"] <= dt_to:
                    selected.append(entry["file_path"])
        else:
            dt_from = now_utc - timedelta(minutes=lookback_minutes)
            for entry in source_entries:
                mod = entry.get("modificationTime")
                if mod is not None:
                    mod_dt = datetime.fromtimestamp(mod / 1000, tz=UTC)
                    if mod_dt >= dt_from:
                        selected.append(entry["file_path"])
        return sorted(set(selected))

    raise ValueError(f"Unsupported request_type: {request_type}")


def _row_identity_key(row: dict) -> str | None:
    fp = row.get("file_fingerprint")
    if fp and str(fp).strip():
        return str(fp).strip()
    return row.get("file_path")


def latest_rows_by_identity(file_log_rows: list[dict]) -> dict[str, dict]:
    latest = {}
    for row in sorted(file_log_rows, key=lambda r: r.get("ts_event") or datetime.min.replace(tzinfo=UTC), reverse=True):
        key = _row_identity_key(row)
        if key and key not in latest:
            latest[key] = row
    return latest


def find_blocked_recent_paths(file_log_rows: list[dict]) -> list[str]:
    latest = latest_rows_by_identity(file_log_rows)
    blocked = []
    for key, row in latest.items():
        if row.get("stage_name") == "INGEST_CONTROL" and row.get("stage_status") == "STARTED":
            blocked.append(key)
    return sorted(blocked)


def find_already_done_paths(inventory_rows: list[dict]) -> list[str]:
    fps = []
    for row in inventory_rows:
        if row.get("load_status") == "LOADED_BRONZE" and row.get("file_fingerprint"):
            fps.append(str(row["file_fingerprint"]))
    return sorted(set(fps))


def fingerprint_for_path(path: str, source_path_map: dict[str, dict]) -> str:
    entry = source_path_map.get(str(path)) or {}
    if entry.get("file_fingerprint"):
        return str(entry["file_fingerprint"])
    return compute_file_fingerprint(str(path), source_size(entry), source_mtime_ms(entry))


def classify_request_paths(
    requested_paths, source_entries, cfg, inventory_paths,
    blocked_recent_fingerprints=None, already_done_fingerprints=None,
    force_reprocess=False,
) -> dict:
    blocked_recent_fingerprints = blocked_recent_fingerprints or set()
    already_done_fingerprints = already_done_fingerprints or set()
    source_path_map = {str(x["file_path"]): x for x in source_entries}
    requested = sorted(set(requested_paths))
    pattern_valid, pattern_invalid, exist, missing, in_inventory, not_in_inventory = [], [], [], [], [], []
    for path in requested:
        file_name = source_path_map.get(path, {}).get("name") or PurePosixPath(path.replace("dbfs:", "")).name
        parsed = parse_filename_metadata(file_name, cfg)
        (pattern_valid if parsed["parse_status"] == "PARSED" else pattern_invalid).append(path)
        (exist if path in source_path_map else missing).append(path)
    ready_base = sorted(set(pattern_valid).intersection(exist))
    for path in ready_base:
        (in_inventory if path in inventory_paths else not_in_inventory).append(path)
    blocked_paths, already_done_paths = [], []
    for path in ready_base:
        fp = fingerprint_for_path(path, source_path_map)
        if fp in blocked_recent_fingerprints or path in blocked_recent_fingerprints:
            blocked_paths.append(path)
        if fp in already_done_fingerprints:
            already_done_paths.append(path)
    blocked_set = set(blocked_paths)
    already_set = set(already_done_paths)
    if force_reprocess:
        final_eligible = sorted(set(ready_base) - blocked_set)
    else:
        final_eligible = sorted(set(ready_base) - blocked_set - already_set)
    final_rejected = sorted(set(requested) - set(final_eligible))
    return {
        "arr_paths_requested": requested,
        "arr_paths_pattern_valid": sorted(set(pattern_valid)),
        "arr_paths_pattern_invalid": sorted(set(pattern_invalid)),
        "arr_paths_exist": sorted(set(exist)),
        "arr_paths_missing": sorted(set(missing)),
        "arr_paths_blocked_recent": sorted(blocked_set),
        "arr_paths_already_done": sorted(already_set),
        "arr_paths_ready": sorted(set(ready_base)),
        "arr_paths_in_inventory": sorted(set(in_inventory)),
        "arr_paths_not_in_inventory": sorted(set(not_in_inventory)),
        "arr_paths_final_eligible": sorted(set(final_eligible)),
        "arr_paths_final_rejected": sorted(set(final_rejected)),
    }


def build_batch_inputs(final_eligible_paths, source_entries, cfg) -> list[dict]:
    source_map = {str(x["file_path"]): x for x in source_entries}
    limit_count = int(cfg.get("batch_max_files", 10))
    limit_bytes = int(float(cfg.get("batch_max_size_gb", 1.0)) * (1024 ** 3))
    batch_inputs, current_paths, current_size, batch_id = [], [], 0, 1

    def flush():
        nonlocal current_paths, current_size, batch_id
        if not current_paths:
            return
        refs = [{"path": p, "fingerprint": fingerprint_for_path(p, source_map)} for p in current_paths]
        batch_inputs.append({"batch_id": f"batch_{batch_id:03d}", "batch_file_paths_json": json.dumps(refs), "file_count": len(current_paths), "total_size_bytes": current_size})
        batch_id += 1
        current_paths, current_size = [], 0

    for path in final_eligible_paths:
        entry = source_map.get(path, {})
        size = int(entry.get("size") or entry.get("file_size") or 0)
        if len(current_paths) >= limit_count or (current_paths and current_size + size > limit_bytes):
            flush()
        current_paths.append(path)
        current_size += size
    flush()
    return batch_inputs


def build_request_status_struct(final_eligible_paths, request_type) -> dict:
    if request_type == "DISCOVERY":
        status_code = "DISCOVERY_ONLY"
    else:
        status_code = "READY_TO_PROCESS" if final_eligible_paths else "NO_ELIGIBLE_FILES"
    return {"status_code": status_code, "status_ts": datetime.now(UTC)}


def build_request_log_record(feed_key, payload, waterfall, batch_inputs, *, dispatch_run_id=None) -> dict:
    return {
        "request_id": __import__("uuid").uuid4().hex,
        "feed_key": feed_key,
        "dispatch_run_id": dispatch_run_id,
        "request_type": str(payload["request_type"]).upper(),
        "request_payload_json": json.dumps(payload),
        "request_status_struct": build_request_status_struct(waterfall["arr_paths_final_eligible"], str(payload["request_type"]).upper()),
        **{k: waterfall[k] for k in waterfall},
        "arr_batch_inputs": batch_inputs,
        "ts_created": datetime.now(UTC),
        "ts_updated": datetime.now(UTC),
    }
