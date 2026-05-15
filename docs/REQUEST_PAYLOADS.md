<a id="top"></a>

# Request Payloads Cheat Sheet

> Quick reference for all request types supported by the ingestion pipeline.

---

## Table of Contents

 Section | Description |
---------|-------------|
 [INCREMENTAL](#1-incremental--process-new-files) | Process new files with lookback window |
 [BACKFILL](#2-backfill--reprocess-a-date-range) | Reprocess a specific date range |
 [DISCOVERY](#3-discovery--list-files-without-ingesting) | Dry-run file listing |
 [Force Reprocess](#4-force-reprocess--re-ingest-already-loaded-files) | Override already-done check |
 [ADHOC](#5-adhoc--target-specific-files-by-path) | Target specific files by path |
 [Common Lookback Values](#common-lookback-values) | Preset minute conversions |
 [Job Parameters Reference](#job-parameters-reference) | Full parameter list |
 [CLI Examples](#cli-examples) | Bash + PowerShell snippets |

---

Quick reference for `request_json` when running `vendor_ingestion_job`.

**How to use:** Jobs → `vendor_ingestion_job` → **Run now with different parameters** → paste JSON into `request_json`.

---

## 1. INCREMENTAL — Process New Files

### FILE_DATE (select by date in filename)

```json
{"request_type":"INCREMENTAL","selector_type":"FILE_DATE","lookback_minutes":1440,"force_reprocess":false}
```

| Field | Value | Notes |
|-------|-------|-------|
| `request_type` | `INCREMENTAL` | Process only new/unprocessed files |
| `selector_type` | `FILE_DATE` | Select by date parsed from filename |
| `lookback_minutes` | `1440` | 24h window — files with file_date within this window |
| `force_reprocess` | `false` | Skip files already in bronze |

### FILE_MODIFIED_TS (select by file modification time)

```json
{"request_type":"INCREMENTAL","selector_type":"FILE_MODIFIED_TS","lookback_minutes":1440,"force_reprocess":false}
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## 2. BACKFILL — Reprocess a Date Range

### BACKFILL with FILE_DATE

```json
{"request_type":"BACKFILL","selector_type":"FILE_DATE","file_date_from":"2026-01-01","file_date_to":"2026-04-15","force_reprocess":false}
```

| Field | Value | Notes |
|-------|-------|-------|
| `request_type` | `BACKFILL` | Process files in a specific range |
| `selector_type` | `FILE_DATE` | Match by date in filename |
| `file_date_from` | `2026-01-01` | Start of range (inclusive) |
| `file_date_to` | `2026-04-15` | End of range (inclusive) |
| `force_reprocess` | `false` | Set `true` to re-ingest already-loaded files |

### BACKFILL with FILE_MODIFIED_TS

```json
{"request_type":"BACKFILL","selector_type":"FILE_MODIFIED_TS","modified_from_ts":"2026-01-01T00:00:00Z","modified_to_ts":"2026-04-15T23:59:59Z","force_reprocess":false}
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## 3. DISCOVERY — List Files Without Ingesting

```json
{"request_type":"DISCOVERY"}
```

Lists all files in the source directory and writes to `ops_file_discovery_log`. No bronze ingestion occurs. Useful for verifying file patterns before a real run.

---


<p align="right"><a href="#top">↑ back to top</a></p>

## 4. Force Reprocess — Re-ingest Already Loaded Files

Add `"force_reprocess":true` to any INCREMENTAL or BACKFILL payload:

```json
{"request_type":"INCREMENTAL","selector_type":"FILE_DATE","lookback_minutes":10080,"force_reprocess":true}
```

`lookback_minutes: 10080` = 7 days. Combined with `force_reprocess: true`, this reprocesses all files from the last 7 days even if they're already in bronze.

---


<p align="right"><a href="#top">↑ back to top</a></p>

## 5. ADHOC — Target Specific Files by Path

### Single file

```json
{"request_type":"ADHOC","file_path":"/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260415_01_01.txt"}
```

| Field | Value | Notes |
|-------|-------|-------|
| `request_type` | `ADHOC` | Ingest specific files by path |
| `file_path` | Volume path | Single file to ingest |

### Multiple files

```json
{"request_type":"ADHOC","file_paths":["/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260401_01_01.txt","/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260415_01_01.txt"],"force_reprocess":true}
```

| Field | Value | Notes |
|-------|-------|-------|
| `request_type` | `ADHOC` | Ingest specific files by path |
| `file_paths` | Array of paths | Multiple files to ingest |
| `force_reprocess` | `true` | Re-ingest even if already in bronze |

Accepts `file_path` (single string) or `file_paths` (array). Paths can be Volume paths, `dbfs:/` paths, or bare filenames (resolved against the feed's source directory).

---


<p align="right"><a href="#top">↑ back to top</a></p>

## Common Lookback Values

| Minutes | Duration |
|---------|----------|
| `60` | 1 hour |
| `1440` | 1 day |
| `10080` | 7 days |
| `43200` | 30 days |
| `129600` | 90 days |

---


<p align="right"><a href="#top">↑ back to top</a></p>

## Job Parameters Reference

| Parameter | Description |
|-----------|-------------|
| `env` | Environment: `dev`, `test`, `prod` |
| `feed_key` | Feed name (e.g. `retro_status_report_ci_aca`) |
| `request_json` | JSON payload (plain text) |
| `dispatch_run_id` | Correlation ID (auto-set by dispatcher, leave empty for manual runs) |

---


<p align="right"><a href="#top">↑ back to top</a></p>

## CLI Examples

### Bash

```bash
# Default request (uses feed's sys_default_request_json from config)
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca

# Incremental with FILE_DATE (24h lookback)
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"INCREMENTAL","selector_type":"FILE_DATE","lookback_minutes":1440,"force_reprocess":false}'

# Incremental with FILE_MODIFIED_TS (24h lookback)
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"INCREMENTAL","selector_type":"FILE_MODIFIED_TS","lookback_minutes":1440,"force_reprocess":false}'

# Backfill with FILE_DATE (Jan 2026)
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"BACKFILL","selector_type":"FILE_DATE","file_date_from":"2026-01-01","file_date_to":"2026-01-31","force_reprocess":false}'

# Backfill with FILE_MODIFIED_TS (timestamp range)
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"BACKFILL","selector_type":"FILE_MODIFIED_TS","modified_from_ts":"2026-01-01T00:00:00Z","modified_to_ts":"2026-01-31T23:59:59Z","force_reprocess":false}'

# Discovery only
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"DISCOVERY"}'
# ADHOC — single file
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"ADHOC","file_path":"/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260415_01_01.txt"}'

# ADHOC — multiple files with force reprocess
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"ADHOC","file_paths":["/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260401_01_01.txt","/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260415_01_01.txt"],"force_reprocess":true}'
```

### PowerShell

```powershell
# Default request (uses feed's sys_default_request_json from config)
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- --feed_key retro_status_report_ci_aca

# Incremental with FILE_DATE (24h lookback)
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"INCREMENTAL","selector_type":"FILE_DATE","lookback_minutes":1440,"force_reprocess":false}'

# Incremental with FILE_MODIFIED_TS (24h lookback)
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"INCREMENTAL","selector_type":"FILE_MODIFIED_TS","lookback_minutes":1440,"force_reprocess":false}'

# Backfill with FILE_DATE (Jan 2026)
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"BACKFILL","selector_type":"FILE_DATE","file_date_from":"2026-01-01","file_date_to":"2026-01-31","force_reprocess":false}'

# Backfill with FILE_MODIFIED_TS (timestamp range)
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"BACKFILL","selector_type":"FILE_MODIFIED_TS","modified_from_ts":"2026-01-01T00:00:00Z","modified_to_ts":"2026-01-31T23:59:59Z","force_reprocess":false}'

# Discovery only
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"DISCOVERY"}'
# ADHOC — single file
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"ADHOC","file_path":"/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260415_01_01.txt"}'

# ADHOC — multiple files with force reprocess
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"ADHOC","file_paths":["/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260401_01_01.txt","/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260415_01_01.txt"],"force_reprocess":true}'
```


<p align="right"><a href="#top">↑ back to top</a></p>