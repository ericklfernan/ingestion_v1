# Project Timeline

> Chronological record of feature development, documentation, and key
> decisions for the vendor file ingestion framework.
>
> Sources: file modification timestamps, `.assistant_instructions.md` dated
> entries, and session context.
>
> Last updated: 2026-04-23

---

## Day 1 — Apr 15, 2026: Project Scaffolding

**What:** Initial DABs project created via `databricks bundle init`.

**Files:**
* `.gitignore`, `.vscode/settings.json`, `resources/volumes/README.md`

**Milestone:** Empty bundle skeleton established.

---

## Day 2 — Apr 16, 2026: Test Infrastructure

**What:** pytest infrastructure initialized.

**Files:**
* `.pytest_cache/` setup

**Milestone:** Test harness ready for unit tests.

---

## Day 3 — Apr 17, 2026: Config Seeds + Job Definition

**What:** Feed configuration seeds authored (5 feeds), dispatcher job
defined, bundle manifest (`databricks.yml`) configured.

**Files:**
* `seeds/schema/` — 5 schema seed files (retro_status_report variants)
* `resources/jobs/vendor_ingestion_dispatcher_job.yml`
* `databricks.yml`

**Milestone:** First deployable bundle with dispatcher job and 5 feed configs.

---

## Day 4 — Apr 18, 2026: Core Framework Build (65 files)

**What:** Largest single-day output. Full framework codebase authored —
pipeline modules, framework library, notification system, unit tests,
and first reference docs.

### Framework Library (`src/framework/`)
* `constants.py` — enums, status codes, stage names
* `schemas.py` — Spark schemas for all ops tables
* `settings/feed_config.py` — config model + validation
* `settings/environment.py` — env-aware catalog/schema resolution
* `helpers/filename_parser.py` — file naming pattern parser (adjudication tiers)
* `helpers/fingerprint.py` — SHA-256 file identity
* `helpers/zip_handler.py` — ZIP file extraction
* `helpers/sql_helpers.py` — SQL generation utilities
* `helpers/schema_drift.py` — schema drift detection
* `tracking/records.py` — ops table record builders
* `tracking/ddl.py` — ops table DDL definitions
* `notifications/constants.py` — severity, category, event_type enums
* `notifications/notify.py` — notification routing + recipient resolution
* `provision/provision_feed.py` — per-feed bronze table provisioning

### Pipeline Modules (`pipelines/file_ingestion/`)
* `file_ingestion_pipeline.py` — orchestrator entry point
* `discover/filter_eligible_files.py` — waterfall eligibility filter
* `manifest/build_manifest.py` — manifest builder
* `finalize/close_and_summarize.py` — adjudication + summary
* `orchestrate/evaluate_schedule.py` — cron evaluation

### Unit Tests
* `test_filename_parser.py`, `test_notifications.py`, `test_tracking.py`
* `test_schema_drift.py`, `test_column_mapping.py`, `test_resolve_job.py`
* `test_zip_handler.py`, `test_feed_config.py`
* `conftest.py` — shared fixtures

### Documentation
* `docs/REQUEST_PAYLOADS.md` — request JSON schema reference
* `docs/STYLE_GUIDE.md` — code conventions

**Milestone:** Framework feature-complete. All core modules authored with
tests and initial docs.

---

## Day 5 — Apr 19, 2026: Stress Test, Refactor, Documentation Sprint

**What:** Design principles stress-tested and confirmed. Major refactors
(dispatch_state separation, greenfield cleanup). Full documentation suite
authored.

### Morning — Dispatcher + Dispatch State
* `notebooks/005_dispatcher.py` — dispatcher notebook
* `notebooks/002_manifest.py`, `notebooks/004_finalize.py` — pipeline notebooks

### Midday — ops_dispatch_state Build (Design Principle #7)
* `src/framework/tracking/table_names.py` — added dispatch_state table name
* `src/framework/tracking/ddl.py` — DDL for ops_dispatch_state
* `src/framework/schemas.py` — dispatch_state schema
* `pipelines/file_ingestion/orchestrate/scan_config.py` — dispatch state read/write
* `tests/unit/test_dispatcher.py` — dispatcher unit tests
* `docs/DISPATCH_STATE.md` — dispatch state reference

### Afternoon — Core Pipeline Completion
* `pipelines/file_ingestion/orchestrate/dispatch_feeds.py` — full dispatcher logic
* `pipelines/file_ingestion/discover/run_request_intake.py` — request intake
* `tests/unit/test_request_filter.py` — request filter tests

### Evening — Documentation Sprint
* `docs/ARCHITECTURE.md` — system architecture
* `docs/RUNBOOK.md` — operational runbook
* `docs/SCENARIOS.md` — 22 scenario walkthroughs (S01–S22)
* `docs/CONFIG_SCHEMA.md` — config column reference
* `REFACTOR_PROGRESS.md` — backlog tracking

### Key Decisions
* 23 design principles confirmed via stress test audit
* Greenfield cleanup completed (removed legacy migration code)
* `ops_dispatch_state` built and deployed (principle #7)
* `max_concurrent_runs` incident — changed 10→1 without proposal, caused
  production slowness, reverted. Led to HARD RULE: propose before applying.

**Milestone:** Stress test passed. 23 principles locked. Full doc suite
authored. Stable baseline declared.

---

## Day 6 — Apr 20, 2026: Deployment + Final Stabilization

**What:** Ingestion job YAML finalized, bronze writer and table provisioning
updated. Bundle deployed to dev target.

**Files:**
* `resources/jobs/vendor_ingestion_job.yml` — ingestion job definition
* `pipelines/file_ingestion/ingest/write_to_bronze.py` — bronze writer
* `src/framework/provision/create_tables.py` — table provisioning

### Key Decisions
* Current stable baseline declared — no changes without explicit approval
* Future features require user-allocated branch name

**Milestone:** Bundle deployed and running. Dispatcher every 5 min,
ingestion on-demand per feed.

---

## Day 7 — Apr 21, 2026: (No file changes detected)

Likely a review/planning day or offline work.

---

## Day 8 — Apr 22, 2026: Operations + Notification Planning

**What:** Operational procedures authored. Notification channels
investigated and parked (DNS blocker). Config change channels formalized.

### Config Seeds
* `seeds/config/ingestion_config_empty.csv`
* `seeds/config/ingestion_config_file_date_2d_1h.csv`
* `seeds/config/ingestion_config_backfile_file_date_20260401_20260430.csv`

### Documentation
* `docs/NOTIFICATION_CHANNELS_PLAN.md` — Slack/Teams/email plan + DNS findings
* `docs/HISTORIC_BACKFILL_PROCEDURE.md` — step-by-step backfill MOP

### Key Decisions
* Notification channels PARKED — all external DNS blocked from serverless
* Config change channels formalized (3 channels: CSV seed, direct UPDATE, one-shot override)
* Documentation guardrails established (parameterized not hardcoded, prescriptive not diagnostic)
* Unified override surface deferred

**Milestone:** Operational readiness. Backfill procedure and config
management documented.

---

## Day 9 — Apr 23, 2026: Data Dictionary + Cost + Design Principles Doc

**What:** Complete data dictionary authored (10 tables, 148+ columns).
Cost baseline analysis performed. Design principles formalized as
standalone document.

### Pipeline Notebooks Updated
* `notebooks/001_request_intake.py`
* `notebooks/003_ingest_batch.py`

### Documentation
* `docs/DATA_DICTIONARY.md` — 10 tables, 148+ columns, 22 enum columns,
  cross-table relationships, adjudication tier logic
* `docs/COST_BASELINE.md` — idle cost analysis, billing granularity,
  optimization levers, scaling per feed
* `docs/DESIGN_PRINCIPLES.md` — 23 principles with descriptions and
  scenario cross-references

### Key Decisions
* AI assistant cost transparency established (~$0.15-$0.30 per heavy session)
* Serverless billing confirmed as per-second (no minimum charge per run)
* Idle baseline: ~$1.68/day, ~$50/month at list pricing

**Milestone:** Full documentation suite complete. 13 docs across the project.

---

## Summary

| Day | Date | Focus | Files |
| ---: | --- | --- | ---: |
| 1 | Apr 15 | Scaffolding | 3 |
| 2 | Apr 16 | Test infra | 3 |
| 3 | Apr 17 | Config seeds + job def | 10 |
| 4 | Apr 18 | **Core framework build** | **65** |
| 5 | Apr 19 | **Stress test + docs sprint** | **20** |
| 6 | Apr 20 | Deployment + stabilization | 3 |
| 7 | Apr 21 | (review/planning) | 0 |
| 8 | Apr 22 | Ops procedures + notifications | 5 |
| 9 | Apr 23 | Data dictionary + cost + principles | 5 |
| | | **Total unique files** | **114** |

### Documentation Inventory (13 docs)

| Doc | Created | Scope |
| --- | --- | --- |
| ARCHITECTURE.md | Apr 19 | System architecture |
| CONFIG_SCHEMA.md | Apr 19 | Config column reference (31 columns) |
| SCENARIOS.md | Apr 19 | 22 operational walkthroughs |
| RUNBOOK.md | Apr 19 | Operational runbook |
| DISPATCH_STATE.md | Apr 19 | Dispatch state table reference |
| REQUEST_PAYLOADS.md | Apr 18 | Request JSON schema |
| STYLE_GUIDE.md | Apr 18 | Code conventions |
| REFACTOR_PROGRESS.md | Apr 19 | Backlog tracking |
| HISTORIC_BACKFILL_PROCEDURE.md | Apr 22 | Backfill step-by-step |
| NOTIFICATION_CHANNELS_PLAN.md | Apr 22 | Notification plan + DNS findings |
| DATA_DICTIONARY.md | Apr 23 | 10 tables, 148+ columns, 22 enums |
| COST_BASELINE.md | Apr 23 | Idle cost analysis |
| DESIGN_PRINCIPLES.md | Apr 23 | 23 governing invariants |

---

## How AI Was Leveraged

The AI assistant was leveraged throughout the vendor file ingestion
project — from writing the full framework code (dispatcher, ingestion
pipeline, adjudication, notifications) and unit tests, to designing and
stress-testing the 23 design principles that govern the solution. It
produced all operational documentation including the backfill procedure,
data dictionary (148+ columns across 10 tables), config schema reference,
request payload cheat sheet, and cost baseline analysis. It helped
diagnose production issues like files stuck at DISCOVERED due to batch
capacity limits and lookback window precedence, and refined procedures
through review cycles where I corrected framing rather than just facts
(e.g., "parameterized not hardcoded"). It served as a real-time design
partner — catching issues, proposing alternatives, and checkpointing
decisions into persistent memory so context carries across sessions. What
would normally take weeks of coding, documentation, troubleshooting, and
design review was done in focused interactive sessions at roughly
$0.15–$0.30 per session.

---

## Without AI: Estimated Timeline for a Solo Mid-Level Engineer

The following estimates assume a mid-level data engineer, familiar with
Databricks and PySpark, working solo on the same scope delivered in 9
days with AI assistance. Based on industry-standard velocity of
\~100–200 LOC/day of production-quality code with tests.

### Phase-by-Phase Comparison

| Phase | AI-Assisted | Manual (est.) | Notes |
| --- | ---: | ---: | --- |
| Scaffolding + config seeds | 3 days | 2–3 days | Similar — manual setup is straightforward |
| Core framework (65 files) | **1 day** | **8–12 days** | 14 modules + 8 test files + schemas + constants at \~150 LOC/day net |
| Stress test + refactor | **1 day** | **3–5 days** | 23 principles audited across 15+ files + dispatch_state refactor (new table, DDL, schema, 6 file changes) |
| Deployment + stabilization | 1 day | 2–3 days | Job YAML, bronze writer, integration testing, serverless debugging |
| Ops procedures + notifications | 1 day | 3–5 days | Backfill MOP, notification research, config channels — requires domain thinking + writing |
| Documentation (13 docs) | **2 days** | **10–15 days** | Data dictionary alone (148+ columns, 22 enums) is 2–3 days. 22 scenarios with code paths is another 3–5 days. Architecture, runbook, config schema, cost analysis — each is a half-day minimum |
| **Total** | **9 days** | **28–43 days** | |

### Key Multipliers

| Area | AI Advantage | Why |
| --- | --- | --- |
| Code generation | 65 files in 1 session vs 8–12 days | AI produces, developer reviews. Inverted workflow — review is faster than authoring |
| Documentation | 3,000+ lines across 13 docs in 2 days | Documentation requires re-reading code to extract behavior. AI holds the full codebase in context and writes directly from it |
| Cross-cutting consistency | 23 principles verified in minutes | AI scans 15+ files per pass in seconds. Manually, each file read + cross-reference is 15–30 min |
| Context retention | Zero ramp-up between sessions | AI carries full project context in persistent memory. A developer loses 15–30 min per context switch (est. 4–6 switches/day) |
| Sample data generation | Production-like test files in minutes | AI generated realistic overlapping backfills, duplicate configs, versioned file sets, and high-volume batches on demand. Manually crafting these edge cases takes hours per scenario |
| Iterative refinement | Real-time correction loops | Developer says "parameterized not hardcoded" — AI rewrites all affected docs in one pass. Manually, each doc is a separate edit cycle |

### Bottom Line

* **Conservative estimate:** 6–8 weeks for a solo mid-level engineer
* **Realistic estimate:** 8–10 weeks including review cycles and rework
* **AI-assisted actual:** 9 calendar days (not all full working days)
* **Compression factor:** approximately 3–5x
* **AI session cost for entire project:** estimated $1.50–$3.00 total
