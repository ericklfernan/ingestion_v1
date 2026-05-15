<a id="top"></a>

# Scenario Walkthroughs

> Operational walkthroughs for the file ingestion pipeline.
> Each scenario traces the exact code path, names the tables affected, and calls out edge cases.

---

## Table of Contents

 # | Group | Scenarios |
---|-------|-----------|
 1 | [Bootstrap & Configuration](#group-1) | [S01](#s01) · [S02](#s02) · [S03](#s03) |
 2 | [Backfill & Catchup](#group-2) | [S04](#s04) |
 3 | [File Selection & Waterfall](#group-3) | [S05](#s05) · [S06](#s06) · [S07](#s07) · [S08](#s08) |
 4 | [Adjudication & Silver Readiness](#group-4) | [S09](#s09) |
 5 | [Concurrency & Race Conditions](#group-5) | [S10](#s10) · [S11](#s11) |
 6 | [Error Recovery](#group-6) | [S12](#s12) · [S13](#s13) · [S14](#s14) |
 7 | [Schema & Data Edge Cases](#group-7) | [S15](#s15) · [S16](#s16) · [S17](#s17) |
 8 | [Scale & Limits](#group-8) | [S18](#s18) · [S19](#s19) |
 9 | [Operational Tasks](#group-9) | [S20](#s20) · [S21](#s21) |
 10 | [Multi-Environment](#group-10) | [S22](#s22) |
 — | [Feature Gaps](#feature-gaps) | Summary of refactor items identified |

---

<a id="group-1"></a>

## 1 · Bootstrap & Configuration

<a id="s01"></a>

### S01 — Zero Config (Empty `seeds/config/`)

**Setup:** No CSV files in `seeds/config/`. Dispatcher runs on schedule.

**Code path:** `dispatch_feeds.py` → `collect_config_rows_from_disk()` → `disk_rows = []`

 Step | What happens |
:----:|-------------|
 1 | Creates UC schemas (bronze, silver, gold) |
 2 | Creates 8 ops tables (job_log, file_log, inventory, schema_change_log, request_log, discovery_log, notifications, dispatch_state) |
 3 | Writes `job_log`: *"no config rows in seeds/config"* |
 4 | Writes WARNING notification: *"No config rows found"* |
 5 | Returns status OK, exits early |

**Result:** Safe bootstrap. No provisioning, no fan-out, no ingestion triggers.

**Two jobs deploy:** dispatcher (every 5 min, early-exits) and ingestion (on-demand only). No resource waste.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-1">↑ group</a></p>

---

<a id="s02"></a>

### S02 — First Config with Narrow Lookback

**Setup:** Upload `ingestion_config_1440.csv` (24-hour lookback). 10 vendor files exist, all 48 hours old.

**Code path:** `select_paths_from_request()` → INCREMENTAL → `FILE_MODIFIED_TS` → `dt_from = now - 1440 min`

 Step | What happens |
:----:|-------------|
 1 | Dispatcher syncs config to Delta, provisions feed (volume, bronze table, dirs) |
 2 | Triggers ingestion job with default INCREMENTAL request |
 3 | All 10 files have `mod_dt` < `dt_from` → **all filtered out** |
 4 | Returns empty list → condition gate skips all downstream tasks |

**Result:** All 10 files silently skipped. No error — the system did exactly what the config asked.

> **Key lesson:** First deploy with pre-existing files needs a generous lookback or a one-time BACKFILL.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-1">↑ group</a></p>

---

<a id="s03"></a>

### S03 — Widened Lookback via Config Update

**Setup:** Upload `ingestion_config_14400.csv` (10-day lookback). Same 10 files (48h old).

**Code path:** `deduplicate_config_rows()` → latest CSV wins → `merge_sync_config()` → MERGE

 `ctl_sync_config` | Columns updated | Lookback changes? |
:-------------------------:|-----------------|:-----------------:|
 `Y` | All sync columns | ✅ Yes |
 `N` or omitted | Only `uc_source_dir`, `src_uri`, `ctl_demo_seed_policy` | ❌ No |

**Result (when `Y`):** New lookback takes effect. All 10 files fall within window → all 10 picked up.

> **Trap:** If `ctl_sync_config` is not `Y` in the new CSV, the lookback silently stays at the old value and files remain invisible.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-1">↑ group</a></p>

---

<a id="group-2"></a>

## 2 · Backfill & Catchup

<a id="s04"></a>

### S04 — Onboarding a Vendor with 3-Month Backlog

**Setup:** New vendor, 90 files (3 months, 1/day). Config: `FILE_MODIFIED_TS`, lookback 2880 min (48h).

**Problem:** Automated run picks up \~2 files. 88 are stranded outside the lookback window.

#### Backfill approaches

 Approach | Type | Selector | Payload |
----------|------|----------|---------|
 **A** (recommended) | `BACKFILL` | `FILE_DATE` | `{"request_type":"BACKFILL","selector_type":"FILE_DATE","file_date_from":"2026-01-18","file_date_to":"2026-04-16"}` |
 **B** | `BACKFILL` | `FILE_MODIFIED_TS` | `{"request_type":"BACKFILL","selector_type":"FILE_MODIFIED_TS","modified_from_ts":"...","modified_to_ts":"..."}` |
 **C** | `INCREMENTAL` | — | Temporarily set `sched_lookback_minutes: 129600` (90 days), then revert |
 **D** | `ADHOC` | — | `{"request_type":"ADHOC","file_paths":["dbfs:/...","dbfs:/..."]}` |

#### CI/CD trigger methods

 Method | CI/CD? | Notes |
--------|:------:|-------|
 `sys_default_request_json` in CSV | ✅ | Needs 2 deploys: backfill + revert |
 CLI `databricks jobs run-now` | ✅ | 1 deploy + 1 CLI call — **recommended** |
 Databricks UI "Run Now" | ❌ | Manual |

> **Recommendation:** CLI post-deploy + BACKFILL with FILE_DATE. Config stays clean, no revert needed.

> **Git Bash vs PowerShell:** Substantial quoting differences for nested JSON. Use the file-based approach (write JSON to temp file) for CI/CD portability.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-2">↑ group</a></p>

---

<a id="group-3"></a>

## 3 · File Selection & Waterfall

<a id="s05"></a>

### S05 — Waterfall Classification (20 Files, Mixed Eligibility)

**Setup:** 20 requested, 5 missing from source, 6 blocked (STARTED), 3 already done (LOADED_BRONZE).

**Code path:** `classify_request_paths()` in `filter_eligible_files.py`

 Step | Check | In | Removed | Remaining |
:----:|-------|---:|--------:|----------:|
 1 | Requested | — | — | **20** |
 2 | Exists in source? | 20 | 5 → `arr_paths_missing` | **15** |
 3 | Regex + exists | 15 | — | **15** |
 4 | Blocked (STARTED, last 48h) | 15 | 6 → `arr_paths_blocked_recent` | **9** |
 5 | Already done (fingerprint match) | 9 | 3 → `arr_paths_already_done` | **6** |
 6 | **Final eligible** | — | — | **6** |

**Key nuances:**

- Blocked check is **fingerprint-based** — renamed files with same content stay blocked
- **48-hour window** on blocked check — stuck STARTED files self-heal after 48h
- `force_reprocess=True` bypasses already-done but **not** blocked (safety against double-writes)
- `build_batch_inputs()` groups eligible files by `batch_max_files` (default 2) → 3 batches
- `batch_max_per_run` (job parameter, default 20) caps total batches per run — excess deferred to next cycle

<p align="right"><a href="#top">↑ top</a> · <a href="#group-3">↑ group</a></p>

---

<a id="s06"></a>

### S06 — File Versions in Backfill

**Setup:** Two files for same date/part: `..._1_1.txt` (no version) and `..._1_1_v2.txt`.

**Code path:** `parse_filename_metadata()` → `version_rank()` → `_adjudicate_inventory()` in finalize

 File | `file_version_rank` | `rn` | `flg_latest` | `flg_superseded` | `promote_status` |
------|:-------------------:|:----:|:------------:|:----------------:|:----------------:|
 `..._v2.txt` | 2 | 1 | **Y** | N | READY_FOR_SILVER |
 `..._1_1.txt` | 0 | 2 | N | **Y** | NOT_READY |

**Result:** Both load to bronze. Finalize supersedes the older version. Silver picks v2 only.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-3">↑ group</a></p>

---

<a id="s07"></a>

### S07 — Vendor Overwrites a Previously Loaded File

**Setup:** File loaded 2 weeks ago (`fp_old`). Vendor overwrites same path → new size/mtime → `fp_new`.

**Code path:** `compute_file_fingerprint(path|size|mtime)` → new fingerprint ≠ old

 Check | Old (`fp_old`) | New (`fp_new`) |
-------|:--------------:|:--------------:|
 `already_done_fingerprints` | In set | Not in set → **eligible** |
 Manifest MERGE | Untouched | NOT MATCHED → **new INSERT** |
 Finalize ranking | rn=2 → superseded | rn=1 → **latest** |

**Result:** Treated as a new version. Both rows in inventory. Old superseded, new promoted.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-3">↑ group</a></p>

---

<a id="s08"></a>

### S08 — ZIP File Processing

**Code path:** `write_to_bronze.py` → detects `parsed["file_extension"] == "zip"`

 Step | What happens |
:----:|-------------|
 1 | `build_extract_dirs()` → temp dir under `{volume}/temp/{stem}_{token}/` |
 2 | `extract_zip_text_files()` → Python `zipfile` → extracts only `.txt` files |
 3 | Logs `ZIP_EXTRACT / SUCCEEDED` to file_log |
 4 | `spark.read.csv(extracted_paths)` → reads all `.txt` as one DataFrame |
 5 | Single inventory row for the ZIP (not per extracted file) |
 6 | `cleanup_extract_dir()` in `finally` — always runs |

> **Limitation:** Only `.txt` files extracted. `.csv`, `.dat` inside the ZIP are silently skipped.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-3">↑ group</a></p>

---

<a id="group-4"></a>

## 4 · Adjudication & Silver Readiness

<a id="s09"></a>

### S09 — Tiered Adjudication (FULL / DATED / BARE)

**Problem:** Original adjudication required `part_group_key` (from `file_date` + `file_part_seq`). Files without these were permanently stuck at `NOT_READY`.

**Solution (R4):** Three-tier auto-detection. Keys are never NULL.

 Tier | When | `part_group_key` | `delivery_group_key` | Silver gate |
------|------|------------------|----------------------|-------------|
 **FULL** | date + seq | `{fk}\|{vendor}\|{lob}\|{date}\|{seq}` | `{fk}\|{vendor}\|{lob}\|{date}` | All parts present + loaded |
 **DATED** | date only | `{fk}\|{vendor}\|{lob}\|{date}` | Same | Latest version + loaded |
 **BARE** | no date | `{fk}\|{file_name}` | `{fk}` | File loaded |

**Code:** `filename_parser.py` (key construction) + `close_and_summarize.py` (finalize SQL gated on `file_part_tot IS NOT NULL`).

<p align="right"><a href="#top">↑ top</a> · <a href="#group-4">↑ group</a></p>

---

<a id="group-5"></a>

## 5 · Concurrency & Race Conditions

<a id="s10"></a>

### S10 — Duplicate Dispatcher Cycles

**Question:** Two dispatchers fire simultaneously. Both see the same feed eligible.

**Safeguards:**

1. `max_concurrent_runs: 1` on ingestion job → second `run_now` is queued
2. Dispatcher checks `_active_ingestion_feed_keys()` → skips feeds with active runs
3. SDK check failure → WARNING logged, `max_concurrent_runs` is the hard backstop

**Gap:** If both dispatchers reach `run_now` before either's run registers, the job queue serializes them. Second run finds most files blocked as STARTED. **No duplicate data, but wasted compute.**

> No refactor needed — current safeguards are sufficient.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-5">↑ group</a></p>

---

<a id="s11"></a>

### S11 — Overlapping Backfill Requests

**Question:** Two BACKFILLs with overlapping date ranges → duplicate bronze rows?

**Safeguards:**
- `already_done_fingerprints`: if first backfill completed, fingerprint blocks second
- `blocked_recent_fingerprints`: if first is still running (STARTED), blocks same file

**Gap:** Race window exists if first finishes between second's listing and waterfall check. Duplicates possible but unlikely.

> See **R9** in [Feature Gaps](#feature-gaps).

<p align="right"><a href="#top">↑ top</a> · <a href="#group-5">↑ group</a></p>

---

<a id="group-6"></a>

## 6 · Error Recovery

<a id="s12"></a>

### S12 — Partial Batch Failure (3 of 6 Files Fail)

**Code path:** `write_to_bronze.py` — per-file `try/except` loop

 Files 1–3 ✅ | Files 4–6 ❌ |
:-------------:|:------------:|
 `INGEST_CONTROL/SUCCEEDED` | `INGEST_CONTROL/FAILED` |
 inventory → `LOADED_BRONZE` | inventory → `DISCOVERED` (unchanged) |
 Rows in bronze | No rows |

**Batch status:** `PARTIAL`.

**Recovery:** Failed files have `FAILED` status (not STARTED) → `find_blocked_recent_paths()` only blocks `STARTED` → **failed files are automatically retried on next run.** No manual intervention.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-6">↑ group</a></p>

---

<a id="s13"></a>

### S13 — Stuck STARTED (48-Hour Self-Heal)

**Setup:** File gets STARTED, job crashes before writing SUCCEEDED or FAILED.

**Code:** file_log filter uses `ts_event >= current_timestamp() - INTERVAL 48 HOURS`

 Time since STARTED | Blocked? | Why |
:-------------------:|:--------:|-----|
 < 48 hours | **Yes** | STARTED is latest within window |
 ≥ 48 hours | **No** | Falls outside window → eligible again |

**Result:** Self-healing. No manual cleanup needed.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-6">↑ group</a></p>

---

<a id="s14"></a>

### S14 — Bronze Table Accidentally Dropped

**Current:** No reconciliation. Inventory remains stale at `LOADED_BRONZE`.

**Recovery:** Re-create table (next dispatcher cycle provisions it), then BACKFILL with `force_reprocess: true`.

> See **R10** in [Feature Gaps](#feature-gaps).

<p align="right"><a href="#top">↑ top</a> · <a href="#group-6">↑ group</a></p>

---

<a id="group-7"></a>

## 7 · Schema & Data Edge Cases

<a id="s15"></a>

### S15 — Completely New Schema from Vendor

**Code:** `mergeSchema: true` + `ensure_bronze_business_columns()`

 Situation | Behavior |
-----------|----------|
 New columns in file | `mergeSchema` adds them automatically |
 Columns removed from file | Old columns remain (NULLs for new rows) |
 Schema drift detected | `schema_change_log` row + WARNING notification |

**Result:** Bronze schema grows monotonically. No data loss.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-7">↑ group</a></p>

---

<a id="s16"></a>

### S16 — Empty File (Header Only, 0 Rows)

`df_bronze.count()` returns 0 → `cnt_row_bronze = 0` → finalize: `COALESCE(cnt_row_bronze, 0) <= 0` → **NOT_READY**, status: *"latest but zero rows loaded"*.

**Result:** File loads (no error) but is not promoted to silver.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-7">↑ group</a></p>

---

<a id="s17"></a>

### S17 — Special Characters in Column Names

`delta.columnMapping.mode = 'name'` + `quote_ident()` (backtick-quoting with embedded backtick escaping).

**Handles:** spaces, pipes, Unicode, dots, hyphens — all safe.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-7">↑ group</a></p>

---

<a id="group-8"></a>

## 8 · Scale & Limits

<a id="s18"></a>

### S18 — 500 Files Land at Once

`build_batch_inputs()` groups by `batch_max_files` (default 2) and `batch_max_size_gb` (default 1 GB).

500 ÷ 2 = **250 batches**. Each is a separate job task.

> **Concern:** 250 tasks is heavy. Increase `batch_max_files` for bulk feeds, or see **R11** in [Feature Gaps](#feature-gaps) for a `max_batches_per_run` cap.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-8">↑ group</a></p>

---

<a id="s19"></a>

### S19 — Large File (50M Rows)

Spark handles natively — distributed read/write.

**Concern:** `df_bronze.count()` triggers a full scan *before* the write, doubling the read. Consider write-first, then count from Delta.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-8">↑ group</a></p>

---

<a id="group-9"></a>

## 9 · Operational Tasks

<a id="s20"></a>

### S20 — Decommission a Feed

Set `ctl_active = N` in config CSV → deploy. Dispatcher skips the feed.

**What persists:** Bronze table, volume, inventory rows — no cleanup logic.

> See **R12** in [Feature Gaps](#feature-gaps).

<p align="right"><a href="#top">↑ top</a> · <a href="#group-9">↑ group</a></p>

---

<a id="s21"></a>

### S21 — Rename a `feed_key`

`feed_key` is the primary identity across 7+ tables. Renaming orphans all historical records.

**Current:** No rename utility.

> See **R13** in [Feature Gaps](#feature-gaps).

<p align="right"><a href="#top">↑ top</a> · <a href="#group-9">↑ group</a></p>

---

<a id="group-10"></a>

## 10 · Multi-Environment

<a id="s22"></a>

### S22 — Same Config CSV for Dev and Prod

**Code:** `apply_environment_policy()` in `feed_config.py`

 Setting | Dev | Prod |
---------|-----|------|
 `require_src_uri` | `False` (managed volume) | `True` (raises error if missing) |
 `src_uri` | Empty → auto `/Volumes/...` | S3/ADLS path |

**Isolation:** Separate UC catalogs per environment → separate ops tables, bronze tables. No cross-contamination.

<p align="right"><a href="#top">↑ top</a> · <a href="#group-10">↑ group</a></p>

---

<a id="feature-gaps"></a>

## Feature Gaps Identified

 ID | Feature | Priority | Impact | Scenario |
----|---------|:--------:|:------:|:--------:|
 R1 | Partitioned bronze writes | — | — | ✅ Done |
 R4 | Tiered adjudication | — | — | ✅ Done |
 **R9** | Bronze dedup (overlapping backfills) | 🟢 Low | 🟢 Low | [S11](#s11) |
 **R10** | Inventory reconciliation | 🟢 Low | 🟢 Low | [S14](#s14) |
 **R11** | `max_batches_per_run` cap | 🟡 Medium | 🟡 Medium | [S18](#s18) |
 **R12** | Feed decommission utility | 🟡 Medium | 🟡 Medium | [S20](#s20) |
 **R13** | `feed_key` rename | 🟢 Low | 🟢 Low | [S21](#s21) |

> Full backlog with descriptions: see [REFACTOR_PROGRESS.md → Refactor Backlog](../REFACTOR_PROGRESS.md#refactor-backlog)

<p align="right"><a href="#top">↑ back to top</a></p>
