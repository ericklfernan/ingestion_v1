<a id="top"></a>

# Dispatch State

> Runtime scheduling state for the file ingestion dispatcher.
> Separated from config to keep `ops_cfg_file_ingestion` a pure config registry.

---

## Table of Contents

 Section | Description |
---------|-------------|
 [Design Rationale](#design-rationale) | Why scheduling state lives outside the config table |
 [Table Schema](#table-schema) | Column definitions for `ops_dispatch_state` |
 [Read / Write Flow](#read--write-flow) | How the dispatcher reads and writes dispatch state |
 [Relationship to Config](#relationship-to-config) | What stays in config vs what moves here |
 [Evidence SQL](#evidence-sql) | Diagnostic queries |

---

<a id="design-rationale"></a>

## Design Rationale

**Problem:** The original design stored `last_dispatched_at` directly in `ops_cfg_file_ingestion`. This mixed runtime scheduling state with feed configuration definitions — two concerns with different lifecycles:

 Concern | Lifecycle | Changed by |
---------|-----------|------------|
 Feed config | Slow — changes on deploy or manual edit | Developer / CI/CD |
 Dispatch state | Fast — changes every dispatcher cycle | Dispatcher (automated) |

**Consequences of mixing them:**

- Config sync (`merge_sync_config`) had to carefully avoid overwriting `last_dispatched_at` during MERGE
- `ctl_sync_config=Y` full-sync risked resetting scheduling state
- No clean way to add forward-looking state (e.g. `next_dispatched_at`) without further polluting the config table
- Config table history (Delta time travel) became noisy with timestamp-only updates every 5 minutes

**Solution:** `ops_dispatch_state` — a dedicated ops table that owns all mutable scheduling state. The config table stays a pure registry of feed definitions.

```
┌──────────────────────────────────┐     ┌──────────────────────────────────┐
│   ops_cfg_file_ingestion         │     │   ops_dispatch_state             │
│   (config — slow-changing)       │     │   (state — fast-changing)        │
├──────────────────────────────────┤     ├──────────────────────────────────┤
│ feed_key             PK          │     │ feed_key             PK          │
│ feed_sub_key         PK          │     │ feed_sub_key         PK          │
│ src_file_regex                   │     │ last_dispatched_at               │
│ sched_cron                       │     │ next_dispatched_at               │
│ sched_lookback_minutes           │     │ dispatch_run_id                  │
│ ctl_active                       │     └──────────────────────────────────┘
│ ctl_auto_trigger                 │
│ ctl_sync_config                  │       Joined on (feed_key, feed_sub_key)
│ sys_default_request_json         │       at auto-trigger evaluation time
│ ...                              │
└──────────────────────────────────┘
```

<p align="right"><a href="#top">↑ back to top</a></p>

---

<a id="table-schema"></a>

## Table Schema

**Table:** `{catalog}.{bronze_schema}.ops_dispatch_state`

 Column | Type | Description |
--------|------|-------------|
 `feed_key` | STRING | Feed identifier (PK part 1) |
 `feed_sub_key` | STRING | Sub-key variant (PK part 2, default: `DEFAULT`) |
 `last_dispatched_at` | TIMESTAMP | When the dispatcher last triggered this feed |
 `next_dispatched_at` | TIMESTAMP | When the feed is next expected to run (computed from `sched_cron` at dispatch time) |
 `dispatch_run_id` | STRING | Traceability — links to the dispatcher cycle that last updated this row |

**Primary key:** `(feed_key, feed_sub_key)` — one row per feed.

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.ops_dispatch_state
(
  feed_key STRING, feed_sub_key STRING,
  last_dispatched_at TIMESTAMP, next_dispatched_at TIMESTAMP,
  dispatch_run_id STRING
)
USING DELTA
```

<p align="right"><a href="#top">↑ back to top</a></p>

---

<a id="read--write-flow"></a>

## Read / Write Flow

### Dispatcher cycle (every 5 minutes)

```
005_dispatcher.py → run_dispatcher()
│
├── 1. Sync config (seeds/config/*.csv → ops_cfg_file_ingestion)
├── 2. Ensure ops tables (including ops_dispatch_state)
├── 3. Provision new feeds
│
├── 4. Load dispatch state
│      SELECT feed_key, feed_sub_key, last_dispatched_at
│      FROM ops_dispatch_state
│      → dict keyed by (feed_key, feed_sub_key)
│
├── 5. For each config row:
│      ├── Inject last_dispatched_at from dispatch state dict
│      ├── Evaluate should_auto_trigger_row()
│      │   ├── ctl_active == Y?
│      │   ├── ctl_auto_trigger == Y?
│      │   ├── maintenance_hold expired?
│      │   └── sched_cron cooldown elapsed since last_dispatched_at?
│      │
│      └── If eligible → run_now ingestion job
│          └── MERGE INTO ops_dispatch_state
│              SET last_dispatched_at = current_timestamp(),
│                  next_dispatched_at = <computed from sched_cron>,
│                  dispatch_run_id = <current dispatch_run_id>
│
└── 6. Return summary
```

### Write behavior

 Operation | Trigger | Columns updated |
-----------|---------|-----------------|
 **INSERT** | First dispatch of a new feed | All 5 columns |
 **UPDATE** | Subsequent dispatches | `last_dispatched_at`, `next_dispatched_at`, `dispatch_run_id` |

The dispatcher uses a **MERGE** (upsert) pattern — INSERT if the feed has no row yet, UPDATE if it does.

### Who reads / who writes

 Actor | Reads | Writes |
-------|:-----:|:------:|
 Dispatcher (`dispatch_feeds.py`) | ✅ | ✅ |
 Ingestion job | ✗ | ✗ |
 Operator (SQL queries) | ✅ | ✗ (manual only for emergency reset) |

<p align="right"><a href="#top">↑ back to top</a></p>

---

<a id="relationship-to-config"></a>

## Relationship to Config

### What moved out of `ops_cfg_file_ingestion`

 Column | Old location | New location |
--------|-------------|-------------|
 `last_dispatched_at` | `ops_cfg_file_ingestion` | `ops_dispatch_state` |

### What stays in `ops_cfg_file_ingestion`

Everything else — all `feed_`, `src_`, `tgt_`, `batch_`, `sched_`, `schema_`, `ctl_`, `notify_`, `dir_`, `sys_` columns. The config table remains the source of truth for **what** each feed is and **how** it should behave. The dispatch state table tracks **when** it last ran and **when** it will run next.

### Config CSV impact

`last_dispatched_at` should **not** appear in `seeds/config/*.csv` files. It was never a config property — it was runtime state that was incorrectly co-located with config. Removing it from the CSV keeps the config surface clean.

<p align="right"><a href="#top">↑ back to top</a></p>

---

<a id="evidence-sql"></a>

## Evidence SQL

```sql
-- Current dispatch state for all feeds
SELECT ds.feed_key, ds.feed_sub_key,
       ds.last_dispatched_at, ds.next_dispatched_at,
       ds.dispatch_run_id,
       cfg.sched_cron, cfg.ctl_active, cfg.ctl_auto_trigger
FROM hcb_dev.ri_ops_ra_bronze.ops_dispatch_state ds
JOIN hcb_dev.ri_ops_ra_bronze.ops_cfg_file_ingestion cfg
  ON ds.feed_key = cfg.feed_key AND ds.feed_sub_key = cfg.feed_sub_key
ORDER BY ds.last_dispatched_at DESC;
```

```sql
-- Feeds that have never been dispatched (no dispatch state row)
SELECT cfg.feed_key, cfg.feed_sub_key, cfg.sched_cron, cfg.ctl_active
FROM hcb_dev.ri_ops_ra_bronze.ops_cfg_file_ingestion cfg
LEFT JOIN hcb_dev.ri_ops_ra_bronze.ops_dispatch_state ds
  ON cfg.feed_key = ds.feed_key AND cfg.feed_sub_key = ds.feed_sub_key
WHERE ds.feed_key IS NULL
  AND cfg.ctl_active = 'Y';
```

```sql
-- Overdue feeds (last_dispatched_at older than expected by sched_cron)
SELECT ds.feed_key, ds.feed_sub_key,
       ds.last_dispatched_at, ds.next_dispatched_at,
       current_timestamp() AS now_utc,
       CASE WHEN current_timestamp() > ds.next_dispatched_at THEN 'OVERDUE' ELSE 'ON_SCHEDULE' END AS status
FROM hcb_dev.ri_ops_ra_bronze.ops_dispatch_state ds
ORDER BY ds.next_dispatched_at;
```

```sql
-- Emergency reset: re-trigger a stuck feed on next dispatcher cycle
UPDATE hcb_dev.ri_ops_ra_bronze.ops_dispatch_state
SET last_dispatched_at = NULL, next_dispatched_at = NULL
WHERE feed_key = 'retro_status_report_ci_aca'
  AND feed_sub_key = 'DEFAULT';
```

<p align="right"><a href="#top">↑ back to top</a></p>
