# Data Dictionary

> Complete reference of all tables, columns, valid values, and cross-table
> relationships in the file ingestion framework.
>
> Source of truth: derived from `constants.py`, `schemas.py`, `ddl.py`,
> `records.py`, `notifications/constants.py`, `filename_parser.py`,
> `feed_config.py`, `filter_eligible_files.py`, `write_to_bronze.py`,
> `build_manifest.py`, `close_and_summarize.py`, `dispatch_feeds.py`,
> `schema_drift.py`.
>
> Last updated: 2026-04-23

---

## Table Index

| # | Table | Columns | Enum Columns | Purpose |
| --- | --- | ---: | ---: | --- |
| 1 | `ops_cfg_file_ingestion` | 31 | 8 | Feed configuration (one row per feed) |
| 2 | `ops_file_inventory` | 25 | 4 | File-level tracking (one row per discovered file) |
| 3 | `ops_request_log` | 18 | 2 | Request-level audit (one row per intake request) |
| 4 | `ops_job_log` | 8 | 2 | Task-level event log |
| 5 | `ops_file_log` | 19 | 2 | File-level event log (multi-stage) |
| 6 | `ops_schema_change_log` | 14 | 1 | Schema drift detection log |
| 7 | `ops_discovery_log` | 10 | 0 | File discovery audit trail |
| 8 | `ops_notifications` | 11 | 3 | Notification records |
| 9 | `ops_dispatch_state` | 5 | 0 | Dispatcher scheduling state |
| 10 | `{tgt_bronze_table}` | 7+ | 0 | Bronze data table (one per feed) |
| | **Total** | **148+** | **22** | |

> **Columns** = total columns including framework-added. **Enum Columns** =
> columns with a defined set of valid values (documented below per table).
> Bronze "7+" reflects fixed lineage columns; dynamic columns vary per feed.

---

## 1. ops_cfg_file_ingestion

Feed configuration table. One row per feed_key. Managed by CSV seed + `ctl_sync_config`.

### Control Columns

| Column | Type | Valid Values | Default | Meaning |
| --- | --- | --- | --- | --- |
| `feed_key` | STRING | Any unique identifier | (required) | Primary key — identifies the feed |
| `feed_sub_key` | STRING | Any string | `DEFAULT` | Sub-key for multi-variant feeds |
| `ctl_active` | STRING | `Y`, `N` | `Y` | Feed is active and eligible for processing |
| `ctl_auto_trigger` | STRING | `Y`, `N` | `Y` | Dispatcher can auto-trigger ingestion runs |
| `ctl_sync_config` | STRING | `Y`, `N` | `N` | Whether `bundle deploy` overwrites all columns from CSV |
| `ctl_maintenance_hold_until` | STRING | ISO timestamp or empty | (empty) | Blocks processing until this timestamp passes |
| `ctl_demo_seed_policy` | STRING | `AUTO`, `COPY`, `SKIP` | `AUTO` | How demo seed files are handled during provisioning |

### Source Columns

| Column | Type | Valid Values | Default | Meaning |
| --- | --- | --- | --- | --- |
| `src_file_regex` | STRING | Python regex pattern | (required) | Pattern to match source filenames |
| `src_file_capture_spec` | STRING | `group_index\|column\|type;...` | (required) | Maps regex groups to metadata columns |
| `src_subdir` | STRING | Directory name | `source` | Subdirectory under volume for source files |
| `src_uri` | STRING | URI or empty | (empty) | External source URI (enforced in prod via principle #17) |
| `src_file_delimiter` | STRING | Delimiter character | `\|` | Column delimiter in source files |
| `src_file_has_header` | STRING | `Y`, `N` | `Y` | Whether source files have a header row |

### Target Columns

| Column | Type | Valid Values | Default | Meaning |
| --- | --- | --- | --- | --- |
| `tgt_bronze_table` | STRING | Table name | (required) | Bronze Delta table name |
| `tgt_silver_table` | STRING | Table name | = `tgt_bronze_table` | Silver Delta table name |
| `tgt_gold_table` | STRING | Table name | = `tgt_bronze_table` | Gold Delta table name |
| `tgt_volume` | STRING | Volume name | = `tgt_bronze_table` | UC Volume for file storage |
| `tgt_bronze_partition_cols` | STRING | Comma-separated column names | (empty) | Partition columns for bronze table |

### Batch Columns

| Column | Type | Valid Values | Default | Meaning |
| --- | --- | --- | --- | --- |
| `batch_max_files` | INT | Positive integer | `10` | Max files per batch |
| `batch_max_size_gb` | DOUBLE | Positive number | `1.0` | Max total size per batch (GB) |

### Schedule Columns

| Column | Type | Valid Values | Default | Meaning |
| --- | --- | --- | --- | --- |
| `sched_selector_type` | STRING | `FILE_MODIFIED_TS`, `FILE_DATE` | `FILE_MODIFIED_TS` | How INCREMENTAL selects files |
| `sched_lookback_minutes` | INT | Positive integer | `1440` | Lookback window for INCREMENTAL file selection |
| `sched_cron` | STRING | Cron expression | (empty) | When the dispatcher should fire for this feed |
| `sched_timezone` | STRING | IANA timezone | `UTC` | Timezone for cron evaluation |

### System Columns

| Column | Type | Valid Values | Default | Meaning |
| --- | --- | --- | --- | --- |
| `sys_default_request_json` | STRING | JSON string or empty | (empty) | Default request payload for automated runs |
| `schema_read_policy` | STRING | `FIRST_FILE`, `SEED`, `AUTO` | `FIRST_FILE` | How to determine column schema for bronze table |
| `notify_recipients` | STRING | Email addresses | (empty) | Notification recipients (semicolon-separated) |

### Directory Columns

| Column | Type | Valid Values | Default | Meaning |
| --- | --- | --- | --- | --- |
| `dir_request` | STRING | Directory name | `request` | Request staging directory |
| `dir_temp` | STRING | Directory name | `temp` | Temporary processing directory |
| `dir_discovery` | STRING | Directory name | `discovery` | Discovery staging directory |

### Auto-added by framework (not in CSV)

| Column | Type | Meaning |
| --- | --- | --- |
| `config_source_file` | STRING | CSV filename this row was loaded from |
| `uc_source_dir` | STRING | Resolved UC Volume path |
| `dispatch_run_id` | STRING | Last dispatcher run that synced this row |

---

## 2. ops_file_inventory

File-level tracking table. One row per discovered file (keyed by `file_fingerprint`).

### Identity & Metadata

| Column | Type | Meaning |
| --- | --- | --- |
| `feed_key` | STRING | Feed this file belongs to |
| `request_id` | STRING | Request that discovered this file |
| `dispatch_run_id` | STRING | Dispatcher run ID |
| `feed_sub_key` | STRING | Sub-key from config |
| `file_name` | STRING | Source filename |
| `file_path` | STRING | Full source file path |
| `file_fingerprint` | STRING | SHA-256(path + size + mtime) — universal identity key (principle #9) |
| `file_size` | BIGINT | File size in bytes |
| `src_size` | BIGINT | Original source size |
| `src_mtime_ms` | BIGINT | Source file modification time (ms epoch) |

### Parsed Metadata (from filename regex)

| Column | Type | Meaning |
| --- | --- | --- |
| `vendor_code` | STRING | Vendor identifier parsed from filename |
| `lob_code` | STRING | Line of business parsed from filename |
| `file_date` | DATE | Date parsed from filename |
| `file_part_seq` | INT | Part sequence number (e.g., 1 of 3) |
| `file_part_tot` | INT | Total parts expected |
| `file_version_label` | STRING | Version label (e.g., `UPDATED`, `v2`) |
| `file_version_rank` | INT | Numeric rank: 0=original, 1=UPDATED, N=vN |
| `file_extension` | STRING | File extension |

### Grouping Keys (tiered adjudication)

| Column | Type | Meaning |
| --- | --- | --- |
| `delivery_group_key` | STRING | Groups files for delivery completeness check |
| `part_group_key` | STRING | Groups file parts for version ranking |

**Adjudication tier determines key format:**

| Tier | Condition | `delivery_group_key` | `part_group_key` |
| --- | --- | --- | --- |
| FULL | date + seq present | `feed\|vendor\|lob\|date` | `feed\|vendor\|lob\|date\|seq` |
| DATED | date present, no seq | `feed\|vendor\|lob\|date` | `feed\|vendor\|lob\|date` |
| BARE | no date | `feed_key` | `feed_key\|file_name` |

### Status Columns

| Column | Type | Valid Values | Meaning |
| --- | --- | --- | --- |
| `parse_status` | STRING | `PARSED`, `PARSE_FAILED` | Whether filename regex matched |
| `parse_reason` | STRING | Free text or NULL | Reason for parse failure |
| `ts_discovered` | TIMESTAMP | Auto-set | When file was first discovered |
| `load_status` | STRING | See below | Current ingestion state |
| `cnt_row_bronze` | BIGINT | 0+ or NULL | Row count written to bronze |

#### `load_status` Values

| Value | Set by | Meaning | Next state |
| --- | --- | --- | --- |
| `DISCOVERED` | `build_manifest.py` | File found and registered in inventory | → `LOADED_BRONZE` or stays `DISCOVERED` |
| `PARSE_FAILED` | `build_manifest.py` | Filename didn't match regex — cannot process | Terminal (re-discovered if file changes) |
| `LOADED_BRONZE` | `write_to_bronze.py` | Successfully written to bronze table | Terminal for bronze layer |

> Note: `STARTED` and `FAILED` are tracked in `ops_file_log.stage_status`, not in
> `load_status`. The inventory only records the final outcome.

### Adjudication Columns (set by finalize)

| Column | Type | Valid Values | Meaning |
| --- | --- | --- | --- |
| `flg_latest` | STRING | `Y`, `N` | This file is the latest version in its part group |
| `flg_superseded` | STRING | `Y`, `N` | This file was superseded by a newer version |
| `flg_legit_for_silver` | STRING | `Y`, `N` | Eligible for silver promotion (latest + loaded + rows > 0) |
| `promote_status` | STRING | See below | Silver readiness state |
| `status_reason` | STRING | Free text or NULL | Explanation for promote_status |

#### `promote_status` Values

| Value | Condition | Meaning |
| --- | --- | --- |
| `NOT_READY` | Any of: parse failed, not latest version, not loaded, zero rows, incomplete parts | Not eligible for silver |
| `READY_FOR_SILVER` | All of: parsed + latest version + loaded_bronze + rows > 0 + all parts present | Ready for silver promotion |

---

## 3. ops_request_log

Request-level audit. One row per intake request.

| Column | Type | Meaning |
| --- | --- | --- |
| `request_id` | STRING | Unique request identifier (UUID hex) |
| `feed_key` | STRING | Feed this request is for |
| `dispatch_run_id` | STRING | Dispatcher run that triggered this request |
| `request_type` | STRING | See below |
| `request_payload_json` | STRING | Full JSON payload used for this request |
| `request_status_struct` | STRUCT | `{status_code, status_ts}` — see below |
| `arr_paths_requested` | ARRAY<STRING> | All paths initially selected |
| `arr_paths_pattern_valid` | ARRAY<STRING> | Paths matching the filename regex |
| `arr_paths_pattern_invalid` | ARRAY<STRING> | Paths not matching the regex |
| `arr_paths_exist` | ARRAY<STRING> | Paths confirmed to exist |
| `arr_paths_missing` | ARRAY<STRING> | Paths that no longer exist |
| `arr_paths_blocked_recent` | ARRAY<STRING> | Paths with STARTED status in self-heal window |
| `arr_paths_already_done` | ARRAY<STRING> | Paths already at LOADED_BRONZE |
| `arr_paths_ready` | ARRAY<STRING> | Paths that passed all filters |
| `arr_paths_in_inventory` | ARRAY<STRING> | Paths already in inventory |
| `arr_paths_not_in_inventory` | ARRAY<STRING> | New paths not yet in inventory |
| `arr_paths_final_eligible` | ARRAY<STRING> | Paths selected for processing |
| `arr_paths_final_rejected` | ARRAY<STRING> | Paths rejected with reasons |
| `arr_batch_inputs` | ARRAY<STRUCT> | `{batch_id, batch_file_paths_json, file_count, total_size_bytes}` |
| `ts_created` | TIMESTAMP | Request creation time |
| `ts_updated` | TIMESTAMP | Last update time |

#### `request_type` Values

| Value | Meaning | File selection method |
| --- | --- | --- |
| `INCREMENTAL` | Normal scheduled run (default) | Lookback window via `sched_selector_type` |
| `BACKFILL` | Historic catch-up | Date range or modified timestamp range |
| `ADHOC` | Manual one-shot | Explicit file list |
| `DISCOVERY` | Scan only, no processing | Same as ADHOC but stops after inventory registration |

#### `selector_type` Values (in request payload)

| Value | Used with | Meaning |
| --- | --- | --- |
| `FILE_MODIFIED_TS` | INCREMENTAL, BACKFILL | Select by file modification timestamp |
| `FILE_DATE` | INCREMENTAL, BACKFILL | Select by parsed date in filename |

#### `request_status_struct.status_code` Values

| Value | Meaning |
| --- | --- |
| `READY_TO_PROCESS` | Eligible files found, batches created |
| `NO_ELIGIBLE_FILES` | No files passed filters |
| `DISCOVERY_ONLY` | Discovery request — no processing triggered |

---

## 4. ops_job_log

Task-level event log. Multiple rows per job run.

| Column | Type | Meaning |
| --- | --- | --- |
| `event_id` | STRING | UUID hex |
| `request_id` | STRING | Associated request (nullable) |
| `task_name` | STRING | See below |
| `task_status` | STRING | See below |
| `feed_key` | STRING | Feed key or `dispatcher` |
| `status_reason` | STRING | Human-readable detail |
| `dispatch_run_id` | STRING | Dispatcher run ID |
| `ts_event` | TIMESTAMP | Event timestamp |

#### `task_name` Values

| Value | Written by |
| --- | --- |
| `request_intake` | `run_request_intake.py` |
| `manifest` | `build_manifest.py` |
| `ingest_batch` | `write_to_bronze.py` |
| `finalize` | `close_and_summarize.py` |
| `dispatcher` | `dispatch_feeds.py` |
| `dispatcher_trigger` | `dispatch_feeds.py` |

#### `task_status` Values

| Value | Meaning |
| --- | --- |
| `SUCCEEDED` | Task completed successfully |
| `FAILED` | Task failed with error |
| `SKIPPED` | Task skipped (e.g., feed not eligible, auto-trigger disabled) |

---

## 5. ops_file_log

File-level event log. Multiple rows per file (one per stage transition).

| Column | Type | Meaning |
| --- | --- | --- |
| `event_id` | STRING | UUID hex |
| `request_id` | STRING | Associated request |
| `file_path` | STRING | Source file path |
| `file_fingerprint` | STRING | File identity key |
| `file_name` | STRING | Source filename |
| `feed_key` | STRING | Feed key |
| `stage_name` | STRING | See below |
| `stage_status` | STRING | See below |
| `vendor_code` | STRING | Parsed vendor code |
| `lob_code` | STRING | Parsed LOB code |
| `file_date` | DATE | Parsed file date |
| `file_part_seq` | INT | Part sequence |
| `file_part_tot` | INT | Total parts |
| `file_version_label` | STRING | Version label |
| `file_version_rank` | INT | Version rank |
| `file_extension` | STRING | File extension |
| `status_reason` | STRING | Detail text |
| `cnt_row_written` | BIGINT | Rows written (INGEST_CONTROL only) |
| `dispatch_run_id` | STRING | Dispatcher run ID |
| `ts_event` | TIMESTAMP | Event timestamp |

#### `stage_name` Values

| Value | Written by | Meaning |
| --- | --- | --- |
| `REQUEST_VALIDATION` | `run_request_intake.py` | File evaluated during request filtering |
| `DISCOVERY` | `build_manifest.py` | File registered in inventory |
| `INGEST_CONTROL` | `write_to_bronze.py` | File ingestion lifecycle (STARTED→SUCCEEDED/FAILED) |
| `ADJUDICATION` | `close_and_summarize.py` | Adjudication result after finalize |

#### `stage_status` Values

| Value | Used with stage_name | Meaning |
| --- | --- | --- |
| `STARTED` | `INGEST_CONTROL` | Ingest begun — used for self-heal blocking (principle #13) |
| `SUCCEEDED` | `INGEST_CONTROL`, `DISCOVERY`, `REQUEST_VALIDATION` | Stage completed successfully |
| `FAILED` | `INGEST_CONTROL` | Ingest failed (per-file, doesn't abort batch) |
| `REJECTED` | `REQUEST_VALIDATION` | File rejected during request filtering |
| `READY_FOR_SILVER` | `ADJUDICATION` | File promoted to silver-ready |
| `NOT_READY` | `ADJUDICATION` | File not eligible for silver |

#### Cross-table: self-heal blocking

`stage_name = 'INGEST_CONTROL'` + `stage_status = 'STARTED'` within the
self-heal window (configurable, default 48h) → file appears in
`arr_paths_blocked_recent` in `ops_request_log` → excluded from next run.

---

## 6. ops_schema_change_log

Schema drift detection. One row per file where schema was compared.

| Column | Type | Meaning |
| --- | --- | --- |
| `event_id` | STRING | UUID hex |
| `request_id` | STRING | Associated request |
| `file_path` | STRING | Source file path |
| `file_fingerprint` | STRING | File identity key |
| `file_name` | STRING | Source filename |
| `feed_key` | STRING | Feed key |
| `target_table` | STRING | Bronze table name |
| `source_columns_csv` | STRING | Comma-separated columns found in file |
| `target_columns_csv` | STRING | Comma-separated columns in target table |
| `missing_in_file` | ARRAY<STRING> | Columns in target but not in file |
| `new_in_file` | ARRAY<STRING> | Columns in file but not in target |
| `change_detected` | STRING | `Y` or `N` |
| `status_reason` | STRING | Description of drift or `source columns match target schema` |
| `dispatch_run_id` | STRING | Dispatcher run ID |
| `ts_event` | TIMESTAMP | Event timestamp |

#### `change_detected` Values

| Value | Meaning |
| --- | --- |
| `Y` | Schema drift detected — columns differ between file and target |
| `N` | No drift — columns match, or header disabled |

---

## 7. ops_discovery_log

File discovery audit trail. One row per file per discovery event.

| Column | Type | Meaning |
| --- | --- | --- |
| `event_id` | STRING | UUID hex |
| `request_id` | STRING | Associated request |
| `feed_key` | STRING | Feed key |
| `file_path` | STRING | Source file path |
| `file_fingerprint` | STRING | File identity key |
| `file_name` | STRING | Source filename |
| `request_payload_json` | STRING | Request payload that triggered discovery |
| `status_reason` | STRING | Discovery outcome detail |
| `dispatch_run_id` | STRING | Dispatcher run ID |
| `ts_event` | TIMESTAMP | Event timestamp |

---

## 8. ops_notifications

Notification records. One row per notification event.

| Column | Type | Meaning |
| --- | --- | --- |
| `event_id` | STRING | UUID hex |
| `severity` | STRING | See below |
| `category` | STRING | See below |
| `event_type` | STRING | See below |
| `feed_key` | STRING | Feed key (nullable for system-level events) |
| `dispatch_run_id` | STRING | Dispatcher run ID |
| `request_id` | STRING | Associated request |
| `message` | STRING | Human-readable notification message |
| `details_json` | STRING | Additional context as JSON |
| `resolved_recipients` | STRING | Resolved email recipients |
| `ts_event` | TIMESTAMP | Event timestamp |

#### `severity` Values

| Value | Meaning |
| --- | --- |
| `INFO` | Informational — normal operations |
| `WARNING` | Warning — non-critical issue (e.g., duplicate config resolved) |
| `ERROR` | Error — action required |

#### `category` Values

| Value | Meaning |
| --- | --- |
| `DISPATCHER` | Dispatcher-related events |
| `INGESTION` | Ingestion pipeline events |
| `CONFIG` | Configuration issues |
| `SCHEMA` | Schema drift events |
| `PARSE` | Filename parsing issues |

#### `event_type` Values

| Value | Category | Severity | Meaning |
| --- | --- | --- | --- |
| `DUPLICATE_CONFIG_RESOLVED` | CONFIG | WARNING | Duplicate feed_key in CSV resolved (earliest row wins) |
| `EMPTY_CONFIG` | CONFIG | WARNING | No active config rows found |
| `NO_ELIGIBLE_FILES` | INGESTION | INFO | Request found no files to process |
| `SCHEMA_DRIFT_DETECTED` | SCHEMA | WARNING | File columns differ from target schema |
| `SCHEMA_SEED_MISSING` | SCHEMA | WARNING | Schema seed file not found |
| `PARSE_FAILED` | PARSE | WARNING | Filename didn't match configured regex |
| `TRIGGER_FAILED` | DISPATCHER | ERROR | Failed to trigger ingestion job run |
| `TRIGGER_SKIPPED` | DISPATCHER | INFO | Feed skipped (not eligible, maintenance hold, etc.) |
| `JOB_RESOLVE_FAILED` | DISPATCHER | ERROR | Cannot resolve ingestion job ID |
| `AUTO_TRIGGER_DISABLED` | DISPATCHER | INFO | Auto-trigger disabled for this feed |
| `ACTIVE_RUN_GUARD` | DISPATCHER | INFO | Skipped because feed already has an active run |
| `ENV_POLICY_VIOLATION` | CONFIG | ERROR | Environment policy check failed (e.g., missing src_uri in prod) |

---

## 9. ops_dispatch_state

Dispatcher scheduling state. One row per feed_key + feed_sub_key.

| Column | Type | Meaning |
| --- | --- | --- |
| `feed_key` | STRING | Feed key |
| `feed_sub_key` | STRING | Sub-key |
| `last_dispatched_at` | TIMESTAMP | When this feed was last dispatched |
| `next_dispatched_at` | TIMESTAMP | Computed next eligible dispatch time |
| `dispatch_run_id` | STRING | Last dispatcher run that updated this row |

---

## 10. Bronze Tables ({tgt_bronze_table})

One table per feed. Schema is dynamic (from source files) plus fixed lineage columns.

### Fixed Lineage Columns (added by framework)

| Column | Type | Meaning |
| --- | --- | --- |
| `feed_key` | STRING | Feed key |
| `request_id` | STRING | Request that ingested this row |
| `dispatch_run_id` | STRING | Dispatcher run ID |
| `src_file_name` | STRING | Source filename |
| `src_file_path` | STRING | Full source file path |
| `src_file_fingerprint` | STRING | File identity key |
| `ts_ingest` | TIMESTAMP | When the row was written to bronze |

### Dynamic Columns

All remaining columns come from the source file, determined by `schema_read_policy`:

| Policy | Behavior |
| --- | --- |
| `FIRST_FILE` | Schema from the first file in the batch |
| `SEED` | Schema from `seeds/schema/{tgt_bronze_table}.txt` |
| `AUTO` | Automatic selection |

---

## Cross-Table Relationships

```
ops_cfg_file_ingestion (feed_key)
    │
    ├──► ops_dispatch_state (feed_key, feed_sub_key)
    │       Dispatcher updates last/next dispatch times
    │
    ├──► ops_request_log (feed_key, request_id)
    │       One request per feed per intake run
    │       └── request_status_struct.status_code determines if batches are created
    │
    ├──► ops_file_inventory (feed_key, file_fingerprint)
    │       One row per discovered file
    │       ├── load_status drives re-processing eligibility
    │       ├── promote_status set by finalize adjudication
    │       └── flg_latest/flg_superseded set by version ranking
    │
    ├──► ops_file_log (feed_key, file_fingerprint, stage_name)
    │       Multiple rows per file (lifecycle events)
    │       └── INGEST_CONTROL + STARTED within 48h → blocks re-processing
    │
    ├──► ops_job_log (feed_key, task_name)
    │       Multiple rows per run (one per task)
    │
    ├──► ops_schema_change_log (feed_key, file_fingerprint)
    │       One row per schema comparison
    │
    ├──► ops_discovery_log (feed_key, file_fingerprint)
    │       One row per discovery event
    │
    ├──► ops_notifications (feed_key, event_type)
    │       One row per notification event
    │
    └──► {tgt_bronze_table} (feed_key, src_file_fingerprint)
            Data rows with lineage columns
```

### Key Join Paths

| From | To | Join key | Purpose |
| --- | --- | --- | --- |
| `ops_file_inventory` | `ops_file_log` | `file_fingerprint` | Trace file lifecycle events |
| `ops_file_inventory` | `{tgt_bronze_table}` | `file_fingerprint = src_file_fingerprint` | Match inventory to bronze data |
| `ops_request_log` | `ops_file_inventory` | `request_id` | Which files a request discovered |
| `ops_request_log` | `ops_job_log` | `request_id` | Which tasks ran for a request |
| `ops_cfg_file_ingestion` | all tables | `feed_key` | Filter any table by feed |
| `ops_dispatch_state` | `ops_job_log` | `dispatch_run_id` | Trace dispatcher to job events |
