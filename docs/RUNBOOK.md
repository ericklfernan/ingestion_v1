<a id="top"></a>

# Runbook

> Minimal operational runbook for the file ingestion pipeline.

---

## Table of Contents

 Section | Description |
---------|-------------|
 [What runs where](#what-runs-where) | Job overview |
 [Defaults](#defaults-for-examples-below) | Default values for CLI examples |
 [CLI](#cli) | Build, validate, deploy |
 [Notifications](#notifications) | Alert system |
 [Evidence SQL](#evidence-sql) | Diagnostic queries |
 [Dispatcher auto-trigger](#dispatcher-auto-trigger-per-feed) | Per-feed gating |
 [Knobs (quick)](#knobs-quick) | Runtime toggles |
 [Remove one bad feed](#remove-one-bad-feed-manual) | Manual cleanup |
 [Demo reset](#demo-reset-destructive) | Full reset |
 [Bundle teardown](#bundle-teardown) | Tear down deployment |

---

Authoritative behavior lives in **`src/vendor_ingestion/`**, **`resources/jobs/*.yml`**, and **`databricks.yml`**. This file is a short operator map.

## What runs where

| Piece | Entry |
|--------|--------|
| Catalog / schemas / dispatcher policy | `environment.py` → `ENVIRONMENTS`, `resolve_runtime_settings(env)` |
| Bundle variable for jobs | `databricks.yml` → `variables.env` only (must match `DEFAULT_ENV` / keys in `ENVIRONMENTS`). Job param `feed_key` is **required** — notebooks raise `ValueError` if empty. `DEFAULT_FEED_KEY` is only the widget default for ad-hoc interactive runs. |
| Dispatcher schedule & pause | `resources/jobs/vendor_ingestion_dispatcher_job.yml` (`quartz_cron_expression`, `pause_status`) |
| Job topology & params | `vendor_ingestion_job.yml`, `vendor_ingestion_dispatcher_job.yml`, optional `vendor_ingestion_rollback_job.yml` |

**Dispatcher** (`vendor_ingestion_dispatcher_job` → `005_dispatcher.py` → `run_dispatcher`): ensures UC schemas; loads **`seeds/config/*.csv`** into `ops_cfg_file_ingestion` (append **new** `(feed_key, feed_sub_key)`; merge-refresh `uc_source_dir` / related columns on existing rows); ensures ops tables; **provisions** new active feeds (volume, folders, bronze/silver/gold placeholders, optional seed copy per env + `ctl_demo_seed_policy`, optional schema seed pre-creation per `schema_read_policy`); may **`run_now`** `vendor_ingestion_job` per env flags + config row gates. Returns **`dispatch_run_id`** (stamped on new config rows and ops logs when used).

If **`seeds/config/`** contains no CSV rows (empty folder or header-only files), the dispatcher ensures schemas and ops tables exist, logs the event, and exits cleanly without error — no provisioning or trigger logic runs.

**Duplicate `feed_key` handling:** If the same `(feed_key, feed_sub_key)` appears in multiple CSVs, the **latest file** (by file modified time) wins. If duplicated within the same file, the **earliest row** wins. Dropped duplicates are logged as `WARNING` events (`dispatcher_dedup`) in `ops_job_log`. Non-duplicate feeds are never impacted by duplicates elsewhere.

**Ingestion** (`vendor_ingestion_job`): `request_intake` → `check_eligible_files` (condition task; skips downstream when no files are eligible) → `manifest` → `ingest_for_each` (concurrency **4**) → `finalize`. Params: `env`, `feed_key`, `request_json`, `dispatch_run_id`, `batch_max_per_run` (default 20 — caps total batches per run; deferred files picked up next cycle).

**File identity (greenfield):** Each source file gets a stable **`file_fingerprint`** (SHA-256 over normalized path, size, and modification time; see `fingerprint_core.py`). Intake enriches listings before waterfall logic; **inventory merge**, **“already in bronze”**, **recent in-progress blocks**, and **`mark_bronze_loaded`** key off that fingerprint. **`arr_batch_inputs`** / `for_each_task` pass **`batch_file_paths_json`** as a JSON array of objects: `[{"path":"<dbfs:...>","fingerprint":"<hex>"}, ...]`. Ingest prefers the batch fingerprint; a legacy plain string array is still accepted but should not be used for new work.

**Rollback helper** (optional): `vendor_ingestion_rollback_job` → `006_rollback_cleanup.py` → `run_rollback_cleanup` (defaults **`dry_run=true`**; inspect before setting false).

Current dispatcher schedule in repo: **`0 */5 * * * ?`**, **`timezone_id: UTC`**, **`pause_status: UNPAUSED`** (every 5 minutes unless you change the YAML).


<p align="right"><a href="#top">↑ back to top</a></p>

## Defaults for examples below

Replace if your `env` differs. For `env=dev` in code: catalog **`hcb_dev`**, bronze/ops schema **`ri_ops_ra_bronze`**, example feed **`retro_status_report_ci_aca`**.


<p align="right"><a href="#top">↑ back to top</a></p>

## CLI

### Build, validate, deploy

```bash
python -m pytest tests/unit -q
databricks bundle validate --target dev -p <profile>
databricks bundle deploy --target dev -p <profile>
```

### Dispatcher

```bash
# Bash
databricks bundle run -t dev -p <profile> vendor_ingestion_dispatcher_job \
  -- --env dev
```

```powershell
# PowerShell
databricks bundle run -t dev -p <profile> vendor_ingestion_dispatcher_job -- --env dev
```

### INCREMENTAL — FILE_DATE (24h lookback)

```bash
# Bash
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"INCREMENTAL","selector_type":"FILE_DATE","lookback_minutes":1440,"force_reprocess":false}'
```

```powershell
# PowerShell
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"INCREMENTAL","selector_type":"FILE_DATE","lookback_minutes":1440,"force_reprocess":false}'
```

### INCREMENTAL — FILE_MODIFIED_TS (24h lookback)

```bash
# Bash
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"INCREMENTAL","selector_type":"FILE_MODIFIED_TS","lookback_minutes":1440,"force_reprocess":false}'
```

```powershell
# PowerShell
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"INCREMENTAL","selector_type":"FILE_MODIFIED_TS","lookback_minutes":1440,"force_reprocess":false}'
```

### BACKFILL — FILE_DATE (date range)

```bash
# Bash
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"BACKFILL","selector_type":"FILE_DATE","file_date_from":"2026-04-02","file_date_to":"2026-04-30"}'
```

```powershell
# PowerShell
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"BACKFILL","selector_type":"FILE_DATE","file_date_from":"2026-04-02","file_date_to":"2026-04-30"}'
```

### BACKFILL — FILE_MODIFIED_TS (timestamp range)

```bash
# Bash
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"BACKFILL","selector_type":"FILE_MODIFIED_TS","modified_from_ts":"2026-04-02T00:00:00Z","modified_to_ts":"2026-04-30T23:59:59Z"}'
```

```powershell
# PowerShell
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"BACKFILL","selector_type":"FILE_MODIFIED_TS","modified_from_ts":"2026-04-02T00:00:00Z","modified_to_ts":"2026-04-30T23:59:59Z"}'
```

### DISCOVERY (list files only, no ingestion)

```bash
# Bash
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"DISCOVERY"}'
```

```powershell
# PowerShell
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"DISCOVERY"}'
```



### ADHOC — target specific files by path

Single file:

```bash
# Bash
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"ADHOC","file_path":"/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260415_01_01.txt"}'
```

```powershell
# PowerShell
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"ADHOC","file_path":"/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260415_01_01.txt"}'
```

Multiple files (with force reprocess):

```bash
# Bash
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca \
  --request_json '{"request_type":"ADHOC","file_paths":["/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260401_01_01.txt","/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260415_01_01.txt"],"force_reprocess":true}'
```

```powershell
# PowerShell
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- `
  --feed_key retro_status_report_ci_aca `
  --request_json '{"request_type":"ADHOC","file_paths":["/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260401_01_01.txt","/Volumes/hcb_dev/ri_ops_ra_bronze/retro_status_report_ci_aca/source/CI_Retro Status Report_ACA_20260415_01_01.txt"],"force_reprocess":true}'
```

### Default request (uses feed's `sys_default_request_json` from config)

```bash
# Bash
databricks bundle run -t dev -p <profile> vendor_ingestion_job \
  -- --feed_key retro_status_report_ci_aca
```

```powershell
# PowerShell
databricks bundle run -t dev -p <profile> vendor_ingestion_job -- --feed_key retro_status_report_ci_aca
```

### Utility

Latest run for a job (get numeric **`job_id`** from Workflows UI or `databricks jobs list -p <profile>`):

```powershell
databricks jobs list-runs --job-id <job_id> --limit 1 -p <profile>
```


<p align="right"><a href="#top">↑ back to top</a></p>

## Notifications

The `ops_notifications` table surfaces events that need operator attention — separate from operational logs so they're easy to query and alert on.

**Recipient routing:** Each notification carries a `resolved_recipients` column. In dev/test, the env-level `notification_override_recipients` routes all notifications to the developer. In prod, each feed's `notify_recipients` from `ingestion_config.csv` is used — different feeds can notify different teams/DLs.

| Env | Routing |
|-----|---------|
| `dev` / `test` | `notification_override_recipients` from `environment.py` (overrides all feeds) |
| `prod` | Per-feed `notify_recipients` from config CSV |

Both `notification_override_recipients` and `notify_recipients` support **comma-separated values** for multiple recipients or AD distribution lists (e.g. `"team-a-dl@aetna.com,team-b-dl@aetna.com"`).

| Severity | Meaning |
|----------|---------|
| `INFO` | Noteworthy but expected (e.g. no eligible files, auto-trigger disabled) |
| `WARNING` | Needs review (e.g. duplicate config resolved, schema drift detected, empty config) |
| `ERROR` | Action required (e.g. job resolution failed, trigger failed) |

```sql
-- Recent notifications (last 24h)
SELECT severity, category, event_type, feed_key, resolved_recipients, message, ts_event
FROM hcb_dev.ri_ops_ra_bronze.ops_notifications
WHERE ts_event >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY ts_event DESC;
```

```sql
-- Warnings and errors only
SELECT *
FROM hcb_dev.ri_ops_ra_bronze.ops_notifications
WHERE severity IN ('WARNING', 'ERROR')
ORDER BY ts_event DESC
LIMIT 50;
```

This table can back **Databricks SQL Alerts** for proactive notification (e.g. alert when `severity = 'ERROR'` rows appear).


<p align="right"><a href="#top">↑ back to top</a></p>

## Evidence SQL

```sql
SELECT event_id, task_name, task_status, dispatch_run_id, ts_event
FROM hcb_dev.ri_ops_ra_bronze.ops_job_log
ORDER BY ts_event DESC
LIMIT 30;
```

```sql
SELECT feed_key, stage_name, stage_status, file_name, file_fingerprint, cnt_row_written, ts_event
FROM hcb_dev.ri_ops_ra_bronze.ops_file_log
WHERE feed_key = 'retro_status_report_ci_aca'
ORDER BY ts_event DESC
LIMIT 50;
```

```sql
SELECT file_path, file_fingerprint, load_status, flg_latest, promote_status, status_reason, ts_discovered
FROM hcb_dev.ri_ops_ra_bronze.ops_file_inventory
WHERE feed_key = 'retro_status_report_ci_aca'
ORDER BY ts_discovered DESC
LIMIT 50;
```


<p align="right"><a href="#top">↑ back to top</a></p>

## Dispatcher auto-trigger (per feed)

Evaluated only when the **dispatcher job** runs. Row needs active + auto-trigger + schedule due + no maintenance hold (unless env overrides disable honoring flags — see `environment.py`).

Example SQL to enable fan-out for one feed:

```sql
UPDATE hcb_dev.ri_ops_ra_bronze.ops_cfg_file_ingestion
SET
  ctl_active = 'Y',
  ctl_auto_trigger = 'Y',
  ctl_maintenance_hold_until = '',
  sched_cron = 'HOURLY',
  sched_timezone = 'UTC',
  sys_default_request_json = '{"request_type":"INCREMENTAL","selector_type":"FILE_MODIFIED_TS","lookback_minutes":1440,"force_reprocess":false}'
WHERE feed_key = 'retro_status_report_ci_aca'
  AND feed_sub_key = 'DEFAULT';
```


<p align="right"><a href="#top">↑ back to top</a></p>

## Knobs (quick)

| What | Where |
|------|--------|
| `env` | `databricks.yml` only; must match `ENVIRONMENTS` keys |
| Feed key resolution | `feed_key` is **required** at runtime (notebooks raise `ValueError` if empty). `DEFAULT_FEED_KEY` in `environment.py` is only the widget default for ad-hoc interactive runs. |
| Auto-trigger kill-switch; honor `ctl_auto_trigger`; honor maintenance hold; demo seed copy; `require_src_uri`; test sleep in ingest | `ENVIRONMENTS[*]` in `environment.py` (`ingestion_test_sleep_seconds` is **360** for `test`, **0** for `dev`/`prod` in repo) |
| `ingestion_job_name` / `ingestion_job_id` | `environment.py` (dispatcher resolves id from name unless id set) |
| Per-feed CSV columns | `seeds/config/*.csv` → `dispatcher.py` / `config_core.py` |
| Schema seed pre-creation | `schema_read_policy` in `seeds/config/*.csv`: `FIRST_FILE` (default, columns from first ingested file), `SEED` (error if seed missing), `AUTO` (seed if exists, else skip). Schema files: `seeds/schema/{tgt_bronze_table}.txt` |
| Scenario file generator (local only) | `seeds/scenarios/materialize.py` |


<p align="right"><a href="#top">↑ back to top</a></p>

## Remove one bad feed (manual)

1. Inspect: `SELECT * FROM <catalog>.<bronze_schema>.ops_cfg_file_ingestion WHERE feed_key = '<key>';`
2. Delete ops rows for that `feed_key` (`ops_discovery_log`, `ops_file_schema_change_log`, `ops_file_log`, `ops_request_log`, `ops_job_log`, `ops_file_inventory`) — see table list in `environment.py` docstring / code.
3. `DROP TABLE` bronze/silver/gold feed tables from config row names.
4. `DELETE` config row(s); **remove the row from `seeds/config/*.csv`** or dispatcher will re-append.
5. Optional: `DROP VOLUME` for dedicated `tgt_volume`, or `dbutils.fs.rm` under `/Volumes/<catalog>/<bronze_schema>/<tgt_volume>/` (do not delete external `s3://` / `abfss://` vendor buckets from here).

**By `dispatch_run_id`:** `SELECT feed_key FROM ...ops_cfg_file_ingestion WHERE dispatch_run_id = '<id>';` then per-feed cleanup as above.


<p align="right"><a href="#top">↑ back to top</a></p>

## Demo reset (destructive)

- Optional: drop `ops_cfg_file_ingestion` and reload from seed (notebook: read deployed `.../.bundle/vendor_ingestion/<target>/files/seeds/config/*.csv` into a DataFrame — path uses your user and target).
- Clear files under the feed volume `source` / `request` / `temp` / `discovery`.
- Drop feed bronze/silver/gold tables only — not whole schemas.


<p align="right"><a href="#top">↑ back to top</a></p>

## Bundle teardown

```powershell
databricks bundle destroy --target dev -p <profile> --auto-approve
```


<p align="right"><a href="#top">↑ back to top</a></p>