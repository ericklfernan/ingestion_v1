<a id="top"></a>

# Refactor Progress

> **Branch:** `refactor/framework-pipeline-restructure`
> **Goal:** Decompose monolithic `pipeline_services.py` (1,894 lines) into modular framework + pipeline architecture.

---

## Table of Contents

 Section | Description |
---------|-------------|
 [Measurement](#measurement) | Success criteria and gates |
 [Completed Phases](#completed-phases) | Phase 0–3, tests, notebooks, cleanup |
 [Framework Modules](#framework-modules) | 13 modules, 1,414 lines |
 [Pipeline Stages](#pipeline-stages) | 7 modules, 1,207 lines |
 [Test Suite](#test-suite) | 111 tests, all passing |
 [Decisions](#decisions) | Architectural choices made during refactor |
 [Refactor Backlog](#refactor-backlog) | Prioritized list of remaining work |

---

<a id="measurement"></a>

## Measurement

 Metric | Target | Actual |
--------|--------|--------|
 `pipeline_services.py` line count | 0 | **0** ✅ |
 All tests pass before next phase | Yes | **110/110** ✅ |
 No circular imports | Yes | **Yes** ✅ |
 No missing functions | Yes | **Yes** ✅ |

<p align="right"><a href="#top">↑ back to top</a></p>

---

<a id="completed-phases"></a>

## Completed Phases

### Phase 0: Documentation ✅

- REFACTOR_PROGRESS.md created
- `docs/CONFIG_SCHEMA.md` created (118 lines — 10 prefix groups + old→new mapping)
- Repo folder structure created with READMEs
- `conftest.py` + `pyproject.toml` configured for dual import paths (`src/` and root)
- `pytest.ini` updated (`pythonpath = src .`, `addopts = -p no:cacheprovider`)

### Phase 1–2: Extract Framework ✅

All framework modules extracted — see [Framework Modules](#framework-modules).

### Phase 3: Extract Pipeline Stages ✅

All pipeline stage modules extracted — see [Pipeline Stages](#pipeline-stages).

### Notebooks ✅

All 5 notebooks updated to import via storyteller:

 Notebook | Calls |
----------|-------|
 `001_request_intake.py` | `run_request_intake()` |
 `002_manifest.py` | `run_manifest()` |
 `003_ingest_batch.py` | `run_ingest_batch()` |
 `004_finalize.py` | `run_finalize()` |
 `005_dispatcher.py` | `run_dispatcher()` |

### Cleanup ✅

- Deleted `src/vendor_ingestion/` (12 files, 3,749 lines removed)
- Deleted `config/` placeholder tree (5 READMEs, 4 empty subdirs)
- Updated `docs/ARCHITECTURE.md` (450 lines — new module map, directory layout)

### Bugs Fixed During Migration

1. Added `job_log_missing_columns()` to `framework/tracking/ddl.py`
2. Removed shadowing `tests/unit/framework/` and `tests/unit/pipelines/` dirs
3. Fixed `conftest.py` for workspace filesystem (`sys.dont_write_bytecode = True`)

<p align="right"><a href="#top">↑ back to top</a></p>

---

<a id="framework-modules"></a>

## Framework Modules

> 13 modules · 1,414 lines total

 Module | Lines | Key exports |
--------|------:|-------------|
 `constants.py` | 104 | `BRONZE_DDL`, `CONFIG_COLUMNS` (30), `INVENTORY_COLUMNS` (30) |
 `schemas.py` | 155 | 8 Spark StructType schema builders |
 `helpers/fingerprint.py` | 62 | `compute_file_fingerprint`, `enrich_source_entry` |
 `helpers/filename_parser.py` | 61 | `parse_filename_metadata`, `version_rank` |
 `helpers/zip_handler.py` | 63 | `build_extract_dirs`, `extract_zip_text_files` |
 `helpers/sql_helpers.py` | 18 | `quote_ident`, `sql_string_literal`, `write_rows` |
 `helpers/schema_drift.py` | 39 | `parse_header_columns`, `compare_columns`, `load_schema_seed` |
 `settings/environment.py` | 113 | `ENVIRONMENTS`, `resolve_runtime_settings` |
 `settings/feed_config.py` | 199 | `normalize_config_row`, `folder_paths`, `apply_environment_policy` |
 `tracking/table_names.py` | 35 | 6 table name builders + `core_tables()` |
 `tracking/ddl.py` | 115 | CREATE TABLE DDL, `alter_add_columns_sql` |
 `tracking/records.py` | 117 | `make_job_log_record`, `make_file_log_record` |
 `notifications/notify.py` | 82 | `make_notification`, `resolve_recipients` |
 `provision/create_tables.py` | 133 | `ensure_ops_tables`, `ensure_column_mapping_mode` |
 `provision/provision_feed.py` | 92 | `ensure_feed_environment`, `load_cfg_and_paths` |

<p align="right"><a href="#top">↑ back to top</a></p>

---

<a id="pipeline-stages"></a>

## Pipeline Stages

> 7 modules · 1,207 lines total

 Module | Lines | Key exports |
--------|------:|-------------|
 `orchestrate/evaluate_schedule.py` | 74 | `is_dispatch_due`, `should_auto_trigger_row` |
 `orchestrate/scan_config.py` | 111 | `collect_config_rows_from_disk`, `deduplicate_config_rows` |
 `orchestrate/dispatch_feeds.py` | 270 | `run_dispatcher`, `_try_run_now_ingestion` |
 `discover/filter_eligible_files.py` | 245 | `classify_request_paths`, `build_batch_inputs` |
 `discover/run_request_intake.py` | 110 | `run_request_intake` |
 `manifest/build_manifest.py` | 96 | `run_manifest`, `merge_discovery_rows` |
 `ingest/write_to_bronze.py` | 157 | `run_ingest_batch` (per-file error resilience) |
 `finalize/close_and_summarize.py` | 144 | `run_finalize`, `_adjudicate_inventory` |

<p align="right"><a href="#top">↑ back to top</a></p>

---

<a id="test-suite"></a>

## Test Suite

> **111 tests** · all passing · `python -B -m pytest tests/unit/ -v`

 Test file | Tests | Imports from |
-----------|------:|--------------|
 `test_environment.py` | 5 | `framework.settings.environment` |
 `test_feed_config.py` | 21 | `framework.settings.feed_config`, `framework.constants` |
 `test_schema_drift.py` | 7 | `framework.helpers.schema_drift` |
 `test_zip_handler.py` | 2 | `framework.helpers.zip_handler` |
 `test_tracking.py` | 5 | `framework.tracking.ddl`, `framework.tracking.records` |
 `test_notifications.py` | 16 | `framework.notifications.notify` |
 `test_dispatcher.py` | 31 | `orchestrate.evaluate_schedule`, `orchestrate.scan_config` |
 `test_request_filter.py` | 8 | `discover.filter_eligible_files` |
 `test_filename_parser.py` | 8 | `framework.helpers.filename_parser` |
 `test_column_mapping.py` | 5 | `framework.provision.create_tables` |
 `test_resolve_job.py` | 3 | `orchestrate.dispatch_feeds` |
 **Total** | **111** | |

<p align="right"><a href="#top">↑ back to top</a></p>

---

<a id="decisions"></a>

## Decisions

 Decision | Rationale |
----------|-----------|
 **`config/` deleted, `seeds/` is single config surface** | `seeds/` is synced via `databricks.yml` and discovered at runtime by `locate_seed_root()`. Future pipelines add their own config dirs when code exists to consume them. |
 **Two jobs kept (dispatcher + ingestion)** | `run_now` fan-out works today. Unifying into `for_each` is Phase 5 — orthogonal to the framework/pipeline restructure. |
 **Prevention over rollback** | file_log: STARTED→SUCCEEDED/FAILED. Inventory/schema_change_log: deferred write until bronze success. request_log: write before work, update to FAILED on error. |

<p align="right"><a href="#top">↑ back to top</a></p>

---

<a id="refactor-backlog"></a>

## Refactor Backlog

### Implemented

 ID | Feature | Status | Scenario |
----|---------|--------|----------|
 **R1** | Partitioned bronze writes | ✅ Done | S05 |
 **R4** | Tiered adjudication (FULL/DATED/BARE) | ✅ Done | S09, S12 |
 **R5** | Config column rename | ✅ Done | 10 prefix groups applied across CONFIG_COLUMNS, CSVs, and code |
 **R6** | Job restructure (`for_each`) | ✅ Done | `for_each_task` with `concurrency: 4` for batch parallelism. Job unification deliberately not pursued — separate dispatcher + ingestion is the better pattern. |
 **R13** | `feed_key` rename | ✅ Done | `file_config_key` fully replaced by `feed_key` across all code. Greenfield — no legacy migration needed. |
 **R11** | `batch_max_per_run` job parameter | ✅ Done | Job parameter (default 20). Caps batches per run, deferred files picked up next cycle. |
 **R14** | `ops_dispatch_state` table | ✅ Done | Design principle #7 |

### Planned — Design & Build

 ID | Feature | Important | Urgent | Description |
----|---------|:---------:|:------:|-------------|
 **R2** | Source partition traversal | ✅ Yes | ⬜ No | Deferred. `src_path_template` + recursive path resolver per `feed_key`. Enables S3/ADLS partitioned source reads. |
 **R3** | Row-level data quality flags | ✅ Yes | ⬜ No | Deferred. `dq_status`, `dq_rules_failed` columns on bronze. Waterfall rule evaluation per feed. |
 **R11** | `max_batches_per_run` config cap | ✅ Yes | ✅ Yes | Limit batch count per ingestion run. Prevent 250+ batch scenarios. Defer remaining to next cycle. |

### Planned — Low Priority

 ID | Feature | Important | Urgent | Description |
----|---------|:---------:|:------:|-------------|
 **R7** | Validation & end-to-end test | ✅ Yes | ⬜ No | Bundle deploy, integration test with one real feed. |
 **R8** | Governance, silver, data masking | ✅ Yes | ⬜ No | `framework/governance/`, `pipelines/data_quality/`, `data_masking/`, `silver_transform/` — placeholder READMEs only. |
 **R9** | Bronze dedup — POST-ingest | ✅ Yes | ⬜ No | POST-ingest MERGE/dedup on bronze `src_file_fingerprint`. PRE-ingest dedup (inventory fingerprint filter) is already done. This covers the race window when overlapping backfills bypass the PRE gate. |
 **R10** | Inventory reconciliation | ⬜ No | ⬜ No | Cross-check `LOADED_BRONZE` against actual bronze rows. Recovery utility for accidental table drops. |

### Infrastructure

 ID | Feature | Priority | Impact | Description |
----|---------|:--------:|:------:|-------------|
 **R14** | `ops_dispatch_state` table | ✅ Done | 🟡 Medium | Scheduling state separated from config table. See `docs/DISPATCH_STATE.md`. |

<p align="right"><a href="#top">↑ back to top</a></p>
