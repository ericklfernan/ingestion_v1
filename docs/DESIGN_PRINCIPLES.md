# Design Principles

> 23 governing invariants for the vendor file ingestion framework.
> Confirmed via stress test audit on 2026-04-19.
>
> Last updated: 2026-04-23

---

## Table of Contents

 \# | Group | Principles |
----|-------|------------|
 1 | [Config & Dedup](#group-1) | P01 – P06 |
 2 | [Separation of Concerns](#group-2) | P07 – P08 |
 3 | [File Identity & Error Resilience](#group-3) | P09 – P13 |
 4 | [Request Override Model](#group-4) | P14 – P15 |
 5 | [Environment Isolation](#group-5) | P16 – P18 |
 6 | [Scheduling & Fan-Out](#group-6) | P19 – P20 |
 7 | [Adjudication](#group-7) | P21 – P22 |
 8 | [CLI](#group-8) | P23 |

---

<a id="group-1"></a>

## 1 · Config & Dedup

<a id="p01"></a>

### P01 — `seeds/` is the single config surface

All feed configuration originates from CSV files under `seeds/config/`.
The config table is a downstream artifact — never the source of truth for
onboarding or permanent changes.

**Related scenarios:** [S01](SCENARIOS.md#s01), [S02](SCENARIOS.md#s02)

---

<a id="p02"></a>

### P02 — Across files: latest file by FILE MODIFIED TIME wins

When multiple CSV files in `seeds/config/` define the same `feed_key`,
the row from the file with the latest filesystem modified timestamp wins.

**Related scenarios:** [S03](SCENARIOS.md#s03)

---

<a id="p03"></a>

### P03 — Within same file: earliest row (first encountered) wins

When the same `feed_key` appears multiple times within a single CSV file,
the first row encountered (lowest row number) is kept. Later duplicates
are dropped and logged.

**Related scenarios:** [S03](SCENARIOS.md#s03)

---

<a id="p04"></a>

### P04 — `ctl_sync_config=Y` gates column merge on existing rows

Only feeds with `ctl_sync_config=Y` have their config columns updated
during the dispatcher's MERGE operation. Feeds with `N` retain their
current table values — the CSV row is acknowledged but not applied.

**Related scenarios:** [S02](SCENARIOS.md#s02), [S03](SCENARIOS.md#s03)

---

<a id="p05"></a>

### P05 — Non-duplicate feeds never impacted by duplicates

Dedup resolution for one `feed_key` must never affect the config rows
of other feeds. The dedup logic is scoped per `feed_key`.

**Related scenarios:** [S03](SCENARIOS.md#s03)

---

<a id="p06"></a>

### P06 — Dropped duplicates logged as WARNING in `ops_job_log`

Every dropped duplicate row is recorded in `ops_job_log` with severity
WARNING, including the file path, row number, and feed_key. Silent
drops are never acceptable.

**Related scenarios:** [S03](SCENARIOS.md#s03)

---

<a id="group-2"></a>

## 2 · Separation of Concerns

<a id="p07"></a>

### P07 — `last_dispatched_at` lives in `ops_dispatch_state`, not config

Dispatch scheduling state is runtime metadata, not configuration.
The `ops_dispatch_state` table owns `last_dispatched_at` and
`next_dispatched_at` per feed. The config table stays purely declarative.

**Built:** 2026-04-19

**Related scenarios:** [S10](SCENARIOS.md#s10)

---

<a id="p08"></a>

### P08 — Prevention over rollback (deferred writes)

Inventory and schema_change_log rows are written only after bronze write
succeeds. If bronze fails, no partial state is committed. This eliminates
the need for rollback logic.

**Related scenarios:** [S12](SCENARIOS.md#s12), [S14](SCENARIOS.md#s14)

---

<a id="group-3"></a>

## 3 · File Identity & Error Resilience

<a id="p09"></a>

### P09 — Fingerprint = SHA-256(path + size + mtime)

Every file is identified by a SHA-256 hash of its full path, file size,
and last modified timestamp. This is the universal identity key across
all ops tables.

**Related scenarios:** [S05](SCENARIOS.md#s05), [S07](SCENARIOS.md#s07)

---

<a id="p10"></a>

### P10 — All ops key off fingerprint

Inventory merge, already-done checks, blocked-file checks, and
mark_bronze_loaded all use fingerprint as the join/lookup key.
No operation uses file path alone.

**Related scenarios:** [S05](SCENARIOS.md#s05), [S07](SCENARIOS.md#s07)

---

<a id="p11"></a>

### P11 — Per-file error handling — never abort the batch

If one file fails during ingest, the remaining files in the batch
continue processing. Failures are isolated and logged per file.

**Related scenarios:** [S12](SCENARIOS.md#s12)

---

<a id="p12"></a>

### P12 — Failed files auto-retry on next run

Files with `load_status=FAILED` are automatically eligible for retry
on the next ingestion cycle. Only `STARTED` status blocks a file.

**Related scenarios:** [S12](SCENARIOS.md#s12), [S13](SCENARIOS.md#s13)

---

<a id="p13"></a>

### P13 — Configurable self-heal window for stuck STARTED files

Files stuck in `STARTED` beyond the configured threshold (default 48h)
are automatically reset. The window is parameterized via config — not
hardcoded.

**Related scenarios:** [S13](SCENARIOS.md#s13)

---

<a id="group-4"></a>

## 4 · Request Override Model

<a id="p14"></a>

### P14 — `sys_default_request_json` = automated default

Each feed's default request parameters (request_type, lookback, date
range, etc.) are defined in the config CSV under `sys_default_request_json`.
The dispatcher uses this for every scheduled run.

**Related scenarios:** [S02](SCENARIOS.md#s02), [S04](SCENARIOS.md#s04)

---

<a id="p15"></a>

### P15 — `request_json` job parameter = one-shot override, never persisted

Passing `request_json` as a CLI job parameter overrides the default for
that single run only. The override is never written back to config.
The config CSV remains the permanent baseline.

**Related scenarios:** [S04](SCENARIOS.md#s04)

---

<a id="group-5"></a>

## 5 · Environment Isolation

<a id="p16"></a>

### P16 — Separate UC catalogs per env

Dev, test, and prod each use their own Unity Catalog catalog. No
cross-environment reads or writes. Isolation is structural, not
permission-based.

**Related scenarios:** [S22](SCENARIOS.md#s22)

---

<a id="p17"></a>

### P17 — `require_src_uri` enforced in prod

Production runs require an explicit `src_uri` in the config. This
prevents accidental processing of dev/test source paths in prod.

**Related scenarios:** [S22](SCENARIOS.md#s22)

---

<a id="p18"></a>

### P18 — Notification routing: env-aware

Dev/test notifications route to a single override recipient. Prod
notifications route to per-feed distribution lists defined in config.

**Related scenarios:** [S22](SCENARIOS.md#s22)

---

<a id="group-6"></a>

## 6 · Scheduling & Fan-Out

<a id="p19"></a>

### P19 — Dispatcher evaluates four gates per feed

A feed is triggered only when all four conditions are met:
`sched_cron` (schedule match), `ctl_active=Y`, `ctl_auto_trigger=Y`,
and `ctl_maintenance_hold_until` (not in hold window).

**Related scenarios:** [S01](SCENARIOS.md#s01), [S10](SCENARIOS.md#s10)

---

<a id="p20"></a>

### P20 — `max_concurrent_runs: 10` with per-feed dedup

The ingestion job allows up to 10 concurrent runs for parallel
processing across feeds. Per-feed dedup is handled by
`_active_ingestion_feed_keys()` in the dispatcher — not by the
job-level concurrency limit.

**Related scenarios:** [S10](SCENARIOS.md#s10), [S11](SCENARIOS.md#s11)

---

<a id="group-7"></a>

## 7 · Adjudication

<a id="p21"></a>

### P21 — Three-tier auto-detection: FULL / DATED / BARE

Adjudication tier is determined automatically from the file naming
pattern. FULL keys include feed_key + sub_key + file_date + version.
DATED drops version. BARE uses feed_key + sub_key only.

**Related scenarios:** [S09](SCENARIOS.md#s09)

---

<a id="p22"></a>

### P22 — Version ranking: later versions supersede earlier ones

Within the same file_date and feed_key, higher version numbers take
precedence. The latest version is promoted to silver; earlier versions
are marked as superseded.

**Related scenarios:** [S06](SCENARIOS.md#s06), [S09](SCENARIOS.md#s09)

---

<a id="group-8"></a>

## 8 · CLI

<a id="p23"></a>

### P23 — Use `--` separator style for job parameters

When passing parameters via Databricks CLI, use the `--` separator
to delineate job parameters from CLI flags:

```bash
databricks jobs run-now <job_id> -- --param1 value1 --param2 value2
```

This prevents ambiguity between CLI options and job parameter keys.

---

## Cross-Reference: Principles × Scenarios

| Principle | Scenarios |
| --- | --- |
| P01 | S01, S02 |
| P02 | S03 |
| P03 | S03 |
| P04 | S02, S03 |
| P05 | S03 |
| P06 | S03 |
| P07 | S10 |
| P08 | S12, S14 |
| P09 | S05, S07 |
| P10 | S05, S07 |
| P11 | S12 |
| P12 | S12, S13 |
| P13 | S13 |
| P14 | S02, S04 |
| P15 | S04 |
| P16 | S22 |
| P17 | S22 |
| P18 | S22 |
| P19 | S01, S10 |
| P20 | S10, S11 |
| P21 | S09 |
| P22 | S06, S09 |
| P23 | — |
