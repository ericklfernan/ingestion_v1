<a id="top"></a>

# Architecture

> Modular framework + pipeline architecture for the file ingestion system.

---

## Table of Contents

 Section | Description |
---------|-------------|
 [System Overview](#system-overview) | High-level job diagram |
 [Job 1: Dispatcher](#job-1-dispatcher-vendor_ingestion_dispatcher_job) | Config sync, provisioning, fan-out |
 [Job 2: Ingestion](#job-2-ingestion-vendor_ingestion_job) | 4-task pipeline per feed |
 [Data Flow](#data-flow) | End-to-end data movement |
 [Module Map](#module-map) | Framework + pipeline module tree |
 [Notebook → Module Mapping](#notebook--module-mapping) | Entry point mapping |
 [Notification Flow](#notification-flow) | Soft alerting system |
 [File Identity & Idempotency](#file-identity--idempotency) | Fingerprint design |
 [Request Override Model](#request-override-model) | Request type hierarchy |
 [Environment Controls](#environment-controls) | Dev/test/prod separation |
 [Error-Resilient Ingest](#error-resilient-ingest) | Per-file error handling |
 [Directory Layout](#directory-layout) | Full repo structure |

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Databricks Asset Bundle                             │
│                         databricks.yml (env)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────┐     ┌─────────────────────────────────┐    │
│  │  Dispatcher Job (every 5m)  │     │  Ingestion Job (per feed)       │    │
│  │  005_dispatcher.py          │────>│  Triggered by dispatcher or     │    │
│  │                             │     │  manual run                     │    │
│  └─────────────────────────────┘     └─────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## Job 1: Dispatcher (`vendor_ingestion_dispatcher_job`)

Runs every 5 minutes. Syncs config, provisions feeds, fans out ingestion jobs.

```
┌──────────────────────────────────────────────────────────────────────┐
│                    005_dispatcher.py                                 │
│                    run_dispatcher()                                  │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. Ensure UC schemas exist                                          │
│  2. Load seeds/config/*.csv                                          │
│     ├── Dedup: latest file (by modified time) wins, earliest row wins│
│     └── Empty config? → WARNING notification, exit clean             │
│  3. Sync config → ops_cfg_file_ingestion (merge/append)              │
│  4. Ensure ops tables (job_log, file_log, inventory,                 │
│     file_schema_change_log, notifications, dispatch_state)           │
│  5. Provision each active feed:                                      │
│     ├── Create UC Volume + subdirs (source/request/temp)             │
│     ├── Create bronze/silver/gold tables                             │
│     ├── Apply schema_read_policy (SEED/AUTO/FIRST_FILE)              │
│     └── Copy demo seeds (if env flag set)                            │
│  6. Fan-out: run_now ingestion job per eligible feed                 │
│     ├── Gate: dispatcher_enable_auto_trigger_runs                    │
│     ├── Gate: ctl_active + ctl_auto_trigger                          │
│     ├── Gate: ctl_maintenance_hold_until                             │
│     └── Gate: sched_cron due check                                   │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## Job 2: Ingestion (`vendor_ingestion_job`)

Processes one feed per run. 4-task pipeline with condition gate.

```
┌────────────────────┐
│   request_intake   │  001_request_intake.py
│   run_request_     │  Parse request_json, list source files,
│   intake()         │  apply selector (FILE_DATE / FILE_MODIFIED_TS),
│                    │  fingerprint, adjudicate inventory,
│                    │  set has_eligible_files + batch_inputs
│                    │  cap batches at batch_max_per_run (default 20)
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ check_eligible_    │  condition_task
│ files              │  has_eligible_files == "true"?
│                    │  ├── true  → continue to manifest
└────────┬───────────┘  └── false → skip all downstream (INFO notification)
         │ outcome: true
         ▼
┌────────────────────┐
│   manifest         │  002_manifest.py
│   run_manifest()   │  Build processing manifest,
│                    │  stage metadata for batch processing
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  ingest_for_each   │  003_ingest_batch.py (concurrency: 4)
│  run_ingest_       │  for_each_task over batch_inputs:
│  batch()           │  ├── Read source file (pipe-delimited)
│                    │  ├── Schema drift check → WARNING if mismatch
│                    │  ├── Write to bronze Delta table
│                    │  └── Mark inventory as bronze_loaded
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│   finalize         │  004_finalize.py
│   run_finalize()   │  Adjudicate inventory (version rank, delivery
│                    │  completeness), mark READY_FOR_SILVER, log results
└────────────────────┘
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## Data Flow

```
  seeds/config/*.csv          seeds/schema/*.txt         UC Volume /source/
  (feed definitions)          (column headers)           (vendor files)
        │                           │                          │
        ▼                           ▼                          ▼
┌──────────────┐          ┌──────────────┐          ┌──────────────────┐
│  Dispatcher  │          │  Provisioner │          │  Request Intake  │
│  sync config │─────────>│  create table│          │  list + filter   │
│  dedup rows  │          │  apply schema│          │  fingerprint     │
└──────┬───────┘          └──────────────┘          └────────┬─────────┘
       │                                                     │
       │              ┌──────────────────────┐               │
       │              │   Bronze Delta Table │<──────────────┘
       │              │   (per feed)         │       ingest_batch
       │              └──────────┬───────────┘       writes here
       │                         │
       ▼                         ▼
┌──────────────────────────────────────────────┐
│              Ops Tables (observability)      │
│                                              │
│  ops_cfg_file_ingestion    Config registry   │
│  ops_job_log               Task-level events │
│  ops_file_log              File-level events │
│  ops_file_inventory        File tracking     │
│  ops_notifications         Soft alerts       │
│  ops_dispatch_state        Scheduling state  │
│  ops_file_schema_change_   Schema drift log  │
│    log                                       │
└──────────────────────────────────────────────┘
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## Module Map

The codebase is split into two layers: **framework** (shared infrastructure) and
**pipelines** (business logic organized by stage).

### Framework (`src/framework/`)

Reusable modules with no pipeline-specific logic. Imported by pipeline stages.

```
src/framework/                              1,414 lines total
│
├── constants.py              104
│   ├── BRONZE_DDL                    → Define DDL templates for bronze table creation
│   ├── CONFIG_COLUMNS (29)           → Enumerate all configuration column names for validation
│   ├── INVENTORY_COLUMNS (30)        → Enumerate all inventory table column names
│   ├── BRONZE_LINEAGE_COLUMNS        → List lineage columns appended to every bronze row
│   └── BRONZE_TECHNICAL_COLUMNS      → List technical metadata columns for traceability
│
├── schemas.py                155
│   └── 8 Spark StructType builders   → Define typed schemas for: job_log, file_log,
│                                       schema_change, request_log, discovery_log,
│                                       notification, inventory, config_delta
│
├── helpers/                  243
│   ├── fingerprint.py         62
│   │   ├── compute_file_fingerprint()  → Compute SHA-256 from path+size+mtime as dedup key
│   │   └── enrich_source_entry()       → Attach fingerprint + parsed metadata to raw file record
│   ├── filename_parser.py     61
│   │   ├── parse_filename_metadata()   → Extract date, part_seq, part_tot, vendor, LOB via regex
│   │   ├── version_rank()              → Compute numeric rank for version supersession
│   │   └── capture_spec_list()         → Parse capture_spec config into ordered group names
│   ├── schema_drift.py        39
│   │   ├── parse_header_columns()      → Read first line of source file for column names
│   │   ├── compare_columns()           → Diff incoming vs bronze schema, return added/removed
│   │   └── load_schema_seed()          → Load expected column list from seeds/schema/*.txt
│   ├── zip_handler.py         63
│   │   ├── extract_zip_text_files()    → Extract text files from ZIP archive into temp dir
│   │   └── cleanup_extract_dir()       → Remove temp extraction directory after processing
│   └── sql_helpers.py         18
│       ├── quote_ident()               → Safely quote a SQL identifier to prevent injection
│       ├── sql_string_literal()        → Escape a value for safe use in SQL string literals
│       └── write_rows()                → Insert rows into a Delta table via parameterized SQL
│
├── settings/                 312
│   ├── environment.py        113
│   │   ├── ENVIRONMENTS                → Map environment names to behavior flags and overrides
│   │   ├── resolve_runtime_settings()  → Merge environment defaults with job-level parameters
│   │   ├── DEFAULT_ENV                 → Define fallback environment when none specified
│   │   └── DEFAULT_FEED_KEY            → Define fallback feed_key for standalone testing
│   └── feed_config.py        199
│       ├── normalize_config_row()      → Validate and coerce a raw CSV row into canonical types
│       ├── folder_paths()              → Compute UC Volume paths (source/request/temp) for a feed
│       ├── apply_environment_policy()  → Override config values based on environment rules
│       └── should_copy_demo_seed_files() → Determine whether to copy demo files for sandboxing
│
├── tracking/                 267
│   ├── table_names.py         35
│   │   ├── job_log_table_name()        → Return fully-qualified table name for ops_job_log
│   │   ├── file_log_table_name()       → Return fully-qualified table name for ops_file_log
│   │   └── core_tables()               → Return list of all ops table names for a catalog/schema
│   ├── ddl.py                115
│   │   ├── CREATE TABLE DDL            → Generate CREATE IF NOT EXISTS for all ops tables
│   │   ├── ALTER ADD helpers           → Add missing columns to existing tables defensively
│   │   └── missing column checkers     → Detect columns absent from live table vs expected schema
│   └── records.py            117
│       ├── make_job_log_record()       → Build a structured row for ops_job_log from task context
│       ├── make_file_log_record()      → Build a structured row from file processing result
│       ├── make_schema_change_record() → Build a row capturing column additions/removals
│       └── get_task_value()            → Retrieve a task value set by upstream notebook in job
│
├── notifications/            108
│   ├── constants.py           26
│   │   ├── Severity                    → Define INFO/WARNING/ERROR severity constants
│   │   ├── Categories                  → Define event categories (drift, ingestion, dispatch)
│   │   └── Event types                 → Define specific event type identifiers for routing
│   └── notify.py              82
│       ├── make_notification()         → Construct notification record with severity + message
│       ├── resolve_recipients()        → Determine recipients (env override vs feed-level DLs)
│       ├── notification_table_name()   → Return fully-qualified name for ops_notifications
│       └── notification_create_sql()   → Generate INSERT SQL for writing a notification record
│
├── provision/                225
│   ├── create_tables.py      133
│   │   ├── inventory_create_sql()      → Generate CREATE TABLE DDL for ops_file_inventory
│   │   ├── ensure_column_mapping_mode() → Set column mapping on bronze for schema evolution
│   │   ├── ensure_ops_tables()         → Create all ops tables if they don't exist (idempotent)
│   │   └── ensure_bronze_lineage_columns() → Add lineage columns to bronze if missing
│   └── provision_feed.py      92
│       ├── load_cfg_and_paths()        → Load feed config row and compute all derived paths
│       └── ensure_feed_environment()   → Create UC Volume + subdirs + tables for a feed
│
└── governance/                     (placeholder — future data classification & access)
```

### Pipelines (`pipelines/file_ingestion/`)

Business logic for the file ingestion pipeline, organized by stage.

```
pipelines/file_ingestion/                   1,269 lines total
│
├── file_ingestion_pipeline.py
│   └── Storyteller                     → Document pipeline purpose and re-export all entry points
│
├── orchestrate/              455   Dispatcher stage (005_dispatcher.py)
│   ├── evaluate_schedule.py   74
│   │   ├── is_dispatch_due()           → Evaluate feed's Quartz cron expression against current time
│   │   └── should_auto_trigger_row()   → Check all gates: active, auto_trigger, maintenance, cron
│   ├── scan_config.py        111
│   │   ├── collect_config_rows_from_disk() → Read all CSV files from seeds/config/ directory
│   │   ├── csv_row_to_delta_dict()     → Convert raw CSV row into Delta-compatible typed dict
│   │   ├── deduplicate_config_rows()   → Apply dedup: latest file wins, earliest row wins
│   │   └── merge_sync_config()         → MERGE deduplicated config into ops_cfg_file_ingestion
│   └── dispatch_feeds.py     270
│       ├── run_dispatcher()            → Orchestrate full cycle: sync → provision → fan-out
│       ├── _resolve_ingestion_job_id() → Look up ingestion job ID by name for run_now calls
│       └── _try_run_now_ingestion()    → Fire run_now for one eligible feed with error handling
│
├── discover/                 355   Request intake stage (001_request_intake.py)
│   ├── filter_eligible_files.py  245
│   │   ├── parse_request_payload()     → Parse request_json into mode, date range, path filters
│   │   ├── select_paths_from_request() → Apply selector (FILE_DATE / FILE_MODIFIED_TS) to filter
│   │   ├── classify_request_paths()    → Split paths into new/already-done/blocked via inventory
│   │   ├── find_blocked_recent_paths() → Identify files blocked by recent STARTED status
│   │   ├── build_batch_inputs()        → Group files into batches respecting max_files + max_size
│   │   └── build_request_log_record()  → Construct audit record for ops_request_log
│   └── run_request_intake.py 110
│       └── run_request_intake()        → Orchestrate: parse → list → filter → batch → output
│
├── manifest/                  96   Manifest stage (002_manifest.py)
│   └── build_manifest.py      96
│       ├── discovery_rows()            → Transform raw file metadata into inventory-ready records
│       ├── merge_discovery_rows()      → MERGE discovery records into inventory (PENDING status)
│       └── run_manifest()              → Orchestrate manifest: build rows → merge → log
│
├── ingest/                   157   Ingest stage (003_ingest_batch.py)
│   └── write_to_bronze.py    157
│       └── run_ingest_batch()          → Process batch: read → detect drift → write bronze → mark
│                                         done (error-resilient, per-file, deferred writes)
│
└── finalize/                 144   Finalize stage (004_finalize.py)
    └── close_and_summarize.py  144
        ├── run_finalize()              → Orchestrate adjudication + summarization for the run
        ├── run_rollback_cleanup()      → Clean up partially-written state from a failed prior run
        └── _adjudicate_inventory()     → Rank versions, check completeness, set promote_status
```

### Future Pipelines (Placeholders)

```
pipelines/
├── data_quality/                   Rule-based DQ checks (planned)
├── data_masking/                   PII masking pipeline (planned)
└── silver_transform/               Bronze → silver transformations (planned)
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## Notebook → Module Mapping

All notebooks import through the storyteller (`file_ingestion_pipeline.py`):

```
Notebook                  Entry Point               Pipeline Stage
─────────────────────     ────────────────────────   ─────────────────
005_dispatcher.py         run_dispatcher()           orchestrate/
001_request_intake.py     run_request_intake()       discover/
002_manifest.py           run_manifest()             manifest/
003_ingest_batch.py       run_ingest_batch()         ingest/
004_finalize.py           run_finalize()             finalize/
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## Notification Flow

```
                     ┌────────────────────────┐
                     │   Event occurs         │
                     │   (drift, empty cfg,   │
                     │    no files, etc.)     │
                     └──────────┬─────────────┘
                                │
                     ┌──────────▼──────────────┐
                     │  make_notification()    │
                     │  severity + category    │
                     │  + event_type + message │
                     └──────────┬──────────────┘
                                │
                     ┌──────────▼──────────────┐
                     │  resolve_recipients()   │
                     │  env override wins in   │
                     │  dev/test; feed-level   │
                     │  DLs used in prod       │
                     └──────────┬──────────────┘
                                │
              ┌─────────────────┼─────────────────┐
              ▼                                   ▼
   ┌──────────────────┐                ┌────────────────────┐
   │ ops_notifications│                │ Job Email Alerts   │
   │ (Delta table)    │                │ on_success/failure │
   │                  │                │ (crash alerts only)│
   │ Soft events:     │                │                    │
   │ INFO / WARNING   │                │ Hard events:       │
   │ queryable,       │                │ job passed/failed  │
   │ alertable        │                │                    │
   └──────────────────┘                └────────────────────┘
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## File Identity & Idempotency

```
  Source file on volume
        │
        ▼
  ┌──────────────────────────────┐
  │  compute_file_fingerprint()  │
  │  SHA-256(path + size + mtime)│
  │  → stable hex fingerprint    │
  └──────────────┬───────────────┘
                 │
       ┌─────────┴──────────┐
       ▼                    ▼
┌──────────────┐   ┌───────────────┐
│ Inventory    │   │ Bronze table  │
│ merge by     │   │ dedup by      │
│ fingerprint  │   │ fingerprint   │
│              │   │               │
│ "Is this new │   │ "Already      │
│  or known?"  │   │  loaded?"     │
└──────────────┘   └───────────────┘
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## Request Override Model

```
┌────────────────────────────────┐
│  Config CSV                    │
│  sys_default_request_json      │◀── Source of truth (automated runs)
│  sched_selector_type           │
│  sched_lookback_minutes        │
└───────────────┬────────────────┘
                │ (default)
                ▼
┌────────────────────────────────┐
│  request_json (job parameter)  │◀── One-shot override (manual runs)
│  Passed via UI or CLI          │    Never persisted. Next run
│  Overrides config for this     │    reverts to config defaults.
│  run only.                     │
└────────────────────────────────┘
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## Environment Controls

```
environment.py → ENVIRONMENTS[env]
│
├── copy_demo_seed_files              Sandbox demo data toggle
├── dispatcher_enable_auto_trigger    Global kill-switch for fan-out
├── dispatcher_honor_config_auto_     Respect per-feed ctl_auto_trigger
│   trigger
├── dispatcher_honor_maintenance_hold Respect per-feed maintenance_hold
├── require_src_uri         Enforce external source URI (prod)
├── ingestion_test_sleep_seconds      Artificial delay for stress testing
└── notification_override_recipients  Dev/test: route all to developer
                                      Prod: None (use feed-level DLs)
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## Error-Resilient Ingest

```
run_ingest_batch()
│
├── for each file in batch:
│   │
│   ├── try:
│   │   ├── Write STARTED to file_log
│   │   ├── Read source file (CSV/ZIP)
│   │   ├── Detect schema drift
│   │   ├── Write to bronze Delta table
│   │   ├── Mark inventory as LOADED_BRONZE    ◀── deferred until success
│   │   ├── Write schema_change_log            ◀── deferred until success
│   │   └── Write SUCCEEDED to file_log
│   │
│   └── except:
│       ├── Write FAILED to file_log           ◀── file becomes retryable
│       ├── Track in failed_files list
│       └── continue to next file              ◀── don't abort batch
│
├── finally:
│   └── ZIP cleanup (temp extract dirs)
│
└── Return batch status:
    ├── SUCCEEDED (all files)
    ├── PARTIAL   (some failed)
    └── FAILED    (all failed)
```

---


<p align="right"><a href="#top">↑ back to top</a></p>

## Directory Layout

```
file_ingestion_demo/
├── databricks.yml                        Bundle config (env variable)
├── conftest.py                           pytest path setup
├── pytest.ini                            Test config (pythonpath, addopts)
├── pyproject.toml                        Package metadata
│
├── src/framework/                        Shared infrastructure (1,414 lines)
│   ├── constants.py                      Column lists, DDL templates
│   ├── schemas.py                        Spark StructType builders
│   ├── helpers/                          Pure functions (fingerprint, parsing, ZIP)
│   ├── settings/                         Environment config, feed config
│   ├── tracking/                         Ops table DDL, records, table names
│   ├── notifications/                    Event types, notification builders
│   ├── provision/                        Table creation, feed provisioning
│   └── governance/                       (placeholder)
│
├── pipelines/
│   └── file_ingestion/                   File ingestion pipeline (1,269 lines)
│       ├── file_ingestion_pipeline.py    Storyteller + re-exports
│       ├── orchestrate/                  Dispatcher: schedule, config, fan-out
│       ├── discover/                     Request intake: filter, classify, batch
│       ├── manifest/                     Discovery rows, inventory merge
│       ├── ingest/                       Bronze write, schema drift, error handling
│       └── finalize/                     Adjudication, summarization, rollback
│
├── notebooks/                            Databricks notebook entry points (185 lines)
│   ├── 001_request_intake.py
│   ├── 002_manifest.py
│   ├── 003_ingest_batch.py
│   ├── 004_finalize.py
│   └── 005_dispatcher.py
│
├── seeds/
│   ├── config/*.csv                      Feed definitions
│   ├── schema/*.txt                      Column headers (pipe-delimited)
│   └── source/*.txt, *.zip               Demo source files
│
├── resources/jobs/
│   ├── vendor_ingestion_dispatcher_job.yml
│   ├── vendor_ingestion_job.yml
│   └── vendor_ingestion_rollback_job.yml
│
├── tests/unit/                           102 unit tests (1,122 lines)
│   ├── test_environment.py
│   ├── test_feed_config.py
│   ├── test_schema_drift.py
│   ├── test_zip_handler.py
│   ├── test_tracking.py
│   ├── test_notifications.py
│   ├── test_dispatcher.py
│   ├── test_request_filter.py
│   ├── test_filename_parser.py
│   ├── test_column_mapping.py
│   └── test_resolve_job.py
│
└── docs/
    ├── ARCHITECTURE.md                   This file
    ├── CONFIG_SCHEMA.md                  Column naming (10 prefix groups)
    ├── RUNBOOK.md                        Operator guide
    ├── REQUEST_PAYLOADS.md               Request JSON cheat sheet
    └── DISPATCH_STATE.md                 Scheduling state design & schema
```


<p align="right"><a href="#top">↑ back to top</a></p>