"""Constants shared across all pipelines."""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Bronze table DDL and column metadata
# ---------------------------------------------------------------------------
BRONZE_DDL = """(
  feed_key STRING,
  request_id STRING,
  dispatch_run_id STRING,
  src_file_name STRING,
  src_file_path STRING,
  src_file_fingerprint STRING,
  ts_ingest TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"""

BRONZE_LINEAGE_COLUMNS = [
    ("feed_key", "STRING"),
    ("request_id", "STRING"),
    ("dispatch_run_id", "STRING"),
    ("src_file_fingerprint", "STRING"),
]

BRONZE_TECHNICAL_COLUMNS = {
    "feed_key",
    "request_id",
    "dispatch_run_id",
    "src_file_name",
    "src_file_path",
    "src_file_fingerprint",
    "ts_ingest",
}

# ---------------------------------------------------------------------------
# Feed config column definitions (maps to ops_cfg_file_ingestion)
# ---------------------------------------------------------------------------
CONFIG_COLUMNS = [
    ("feed_key", "STRING"),
    ("feed_sub_key", "STRING"),
    ("ctl_active", "STRING"),
    ("ctl_auto_trigger", "STRING"),
    ("ctl_sync_config", "STRING"),
    ("ctl_maintenance_hold_until", "STRING"),
    ("src_file_regex", "STRING"),
    ("src_file_capture_spec", "STRING"),
    ("src_subdir", "STRING"),
    ("src_uri", "STRING"),
    ("ctl_demo_seed_policy", "STRING"),
    ("dir_request", "STRING"),
    ("dir_temp", "STRING"),
    ("dir_discovery", "STRING"),
    ("src_file_delimiter", "STRING"),
    ("src_file_has_header", "STRING"),
    ("tgt_bronze_table", "STRING"),
    ("tgt_silver_table", "STRING"),
    ("tgt_gold_table", "STRING"),
    ("tgt_volume", "STRING"),
    ("batch_max_files", "INT"),
    ("batch_max_size_gb", "DOUBLE"),
    ("sched_selector_type", "STRING"),
    ("sched_lookback_minutes", "INT"),
    ("sched_cron", "STRING"),
    ("sched_timezone", "STRING"),
    ("sys_default_request_json", "STRING"),
    ("schema_read_policy", "STRING"),
    ("notify_recipients", "STRING"),
    ("tgt_bronze_partition_cols", "STRING"),
]

# ---------------------------------------------------------------------------
# Inventory column definitions (maps to ops_file_inventory)
# ---------------------------------------------------------------------------
INVENTORY_COLUMNS = [
    ("feed_key", "STRING"),
    ("request_id", "STRING"),
    ("dispatch_run_id", "STRING"),
    ("feed_sub_key", "STRING"),
    ("file_name", "STRING"),
    ("file_path", "STRING"),
    ("file_fingerprint", "STRING"),
    ("file_size", "BIGINT"),
    ("src_size", "BIGINT"),
    ("src_mtime_ms", "BIGINT"),
    ("vendor_code", "STRING"),
    ("lob_code", "STRING"),
    ("file_date", "DATE"),
    ("file_part_seq", "INT"),
    ("file_part_tot", "INT"),
    ("file_version_label", "STRING"),
    ("file_version_rank", "INT"),
    ("file_extension", "STRING"),
    ("delivery_group_key", "STRING"),
    ("part_group_key", "STRING"),
    ("parse_status", "STRING"),
    ("parse_reason", "STRING"),
    ("ts_discovered", "TIMESTAMP"),
    ("load_status", "STRING"),
    ("cnt_row_bronze", "BIGINT"),
    ("flg_latest", "STRING"),
    ("flg_superseded", "STRING"),
    ("flg_legit_for_silver", "STRING"),
    ("promote_status", "STRING"),
    ("status_reason", "STRING"),
]
