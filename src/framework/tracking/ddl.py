"""DDL for all tracking/ops tables."""
from __future__ import annotations


def job_log_create_sql(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_name}
(
  event_id STRING, request_id STRING, task_name STRING, task_status STRING,
  feed_key STRING, status_reason STRING, dispatch_run_id STRING, ts_event TIMESTAMP
)
USING DELTA
""".strip()


def file_log_create_sql(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_name}
(
  event_id STRING, request_id STRING, file_path STRING, file_fingerprint STRING,
  file_name STRING, feed_key STRING, stage_name STRING, stage_status STRING,
  vendor_code STRING, lob_code STRING, file_date DATE, file_part_seq INT,
  file_part_tot INT, file_version_label STRING, file_version_rank INT,
  file_extension STRING, status_reason STRING, cnt_row_written BIGINT,
  dispatch_run_id STRING, ts_event TIMESTAMP
)
USING DELTA
""".strip()


def schema_change_log_create_sql(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_name}
(
  event_id STRING, request_id STRING, file_path STRING, file_fingerprint STRING,
  file_name STRING, feed_key STRING, target_table STRING,
  source_columns_csv STRING, target_columns_csv STRING,
  missing_in_file ARRAY<STRING>, new_in_file ARRAY<STRING>,
  change_detected STRING, status_reason STRING, dispatch_run_id STRING, ts_event TIMESTAMP
)
USING DELTA
""".strip()


def request_log_create_sql(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_name}
(
  request_id STRING, feed_key STRING, dispatch_run_id STRING,
  request_type STRING, request_payload_json STRING,
  request_status_struct STRUCT<status_code: STRING, status_ts: TIMESTAMP>,
  arr_paths_requested ARRAY<STRING>, arr_paths_pattern_valid ARRAY<STRING>,
  arr_paths_pattern_invalid ARRAY<STRING>, arr_paths_exist ARRAY<STRING>,
  arr_paths_missing ARRAY<STRING>, arr_paths_blocked_recent ARRAY<STRING>,
  arr_paths_already_done ARRAY<STRING>, arr_paths_ready ARRAY<STRING>,
  arr_paths_in_inventory ARRAY<STRING>, arr_paths_not_in_inventory ARRAY<STRING>,
  arr_paths_final_eligible ARRAY<STRING>, arr_paths_final_rejected ARRAY<STRING>,
  arr_batch_inputs ARRAY<STRUCT<batch_id: STRING, batch_file_paths_json: STRING, file_count: INT, total_size_bytes: BIGINT>>,
  ts_created TIMESTAMP, ts_updated TIMESTAMP
)
USING DELTA
""".strip()


def discovery_log_create_sql(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_name}
(
  event_id STRING, request_id STRING, feed_key STRING,
  file_path STRING, file_fingerprint STRING, file_name STRING,
  request_payload_json STRING, status_reason STRING,
  dispatch_run_id STRING, ts_event TIMESTAMP
)
USING DELTA
""".strip()



def dispatch_state_create_sql(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_name}
(
  feed_key STRING, feed_sub_key STRING,
  last_dispatched_at TIMESTAMP, next_dispatched_at TIMESTAMP,
  dispatch_run_id STRING
)
USING DELTA
""".strip()


def alter_add_columns_sql(table_name: str, missing_columns: list[tuple[str, str]]) -> str:
    cols = ", ".join([f"{n} {t}" for n, t in missing_columns])
    return f"ALTER TABLE {table_name} ADD COLUMNS ({cols})"


def missing_dispatch_run_id_only(existing_columns: list[str]) -> list[tuple[str, str]]:
    existing = {c.lower() for c in existing_columns}
    return [] if "dispatch_run_id" in existing else [("dispatch_run_id", "STRING")]


def job_log_missing_columns(existing_columns: list[str]) -> list[tuple[str, str]]:
    return missing_dispatch_run_id_only(existing_columns)


def file_log_missing_columns(existing_columns: list[str]) -> list[tuple[str, str]]:
    existing = {c.lower() for c in existing_columns}
    return [] if "file_fingerprint" in existing else [("file_fingerprint", "STRING")]


def schema_change_log_missing_columns(existing_columns: list[str]) -> list[tuple[str, str]]:
    existing = {c.lower() for c in existing_columns}
    return [] if "file_fingerprint" in existing else [("file_fingerprint", "STRING")]


def discovery_log_missing_columns(existing_columns: list[str]) -> list[tuple[str, str]]:
    existing = {c.lower() for c in existing_columns}
    return [] if "file_fingerprint" in existing else [("file_fingerprint", "STRING")]


def request_log_missing_columns(existing_columns: list[str]) -> list[tuple[str, str]]:
    cols = [
        ("feed_key", "STRING"),
        ("dispatch_run_id", "STRING"),
        ("arr_paths_already_done", "ARRAY<STRING>"),
        ("arr_batch_inputs", "ARRAY<STRUCT<batch_id: STRING, batch_file_paths_json: STRING, file_count: INT, total_size_bytes: BIGINT>>"),
    ]
    existing = {c.lower() for c in existing_columns}
    return [(n, t) for n, t in cols if n.lower() not in existing]
