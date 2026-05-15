# Historic Backfill Procedure

> Method of procedure for backfilling vendor files via operational UPDATE to
> `sys_default_request_json` and `sched_cron` in the config table. The system
> processes files in batches across multiple cron cycles until all files are
> loaded. Once complete, a bundle deploy reverts the table back to the CSV
> baseline automatically via `ctl_sync_config=Y`.
>
> Last updated: 2026-04-22

---

## The Procedure

### Step 1 — UPDATE the config table

Run this SQL to switch the target feed to BACKFILL and accelerate the cron:

```sql
UPDATE {catalog}.{schema}.ops_cfg_file_ingestion
SET sys_default_request_json = '{"request_type":"BACKFILL","selector_type":"FILE_DATE","file_date_from":"YYYY-MM-DD","file_date_to":"YYYY-MM-DD","force_reprocess":false}',
    sched_cron = '*/15 * * * *'
WHERE feed_key = '{feed_key}'
```

**Rules:**
- Set `file_date_from` to the earliest date you need to backfill.
- Set `file_date_to` to a few days ahead of today to absorb processing lag.
- Set `sched_cron` to an aggressive interval appropriate for the environment.
  Production daily schedules are too slow for backfill — shorten temporarily.
- **`force_reprocess` must be `false`** — this is what makes the multi-cycle
  approach safe. Files already at `LOADED_BRONZE` are automatically skipped.
  Each cron cycle picks up where the last one left off.

> The CSV in seeds/ is **not edited**. Both column UPDATEs are intentionally
> temporal — they will be overwritten on the next bundle deploy.

### Step 2 — Monitor

The next dispatcher cycle picks up the BACKFILL payload and triggers ingestion.
After each cron cycle, check progress:

```sql
SELECT load_status, COUNT(*) AS file_count
FROM {catalog}.{schema}.ops_file_inventory
WHERE feed_key = '{feed_key}'
  AND file_date BETWEEN '{date_from}' AND '{date_to}'
GROUP BY load_status
```

**Done when `DISCOVERED = 0`.**

Each cycle processes up to `batch_max_per_run × batch_max_files` files. Use
this to estimate total time:
`ceil(total_files / files_per_run) × cron_interval`.

### Step 3 — Deploy to revert

Once all files are loaded, deploy the bundle:

```bash
databricks bundle deploy --target {env} -p {profile}
```

The deploy syncs the CSV config to the table via `ctl_sync_config=Y`,
overwriting both `sys_default_request_json` and `sched_cron` back to the
original CSV values. The system resumes normal processing on the next
dispatcher cycle.

> **The deploy IS the revert.** No CSV edit needed. The CSV still has
> INCREMENTAL and the original cron schedule — the deploy simply restores both.

---

## Why This Works

The config sync MERGE in `scan_config.py` has two paths:

- `ctl_sync_config = 'Y'` → **all columns** overwritten from CSV, including
  `sys_default_request_json` and `sched_cron`
- `ctl_sync_config = 'N'` → only `uc_source_dir`, `src_uri`,
  `ctl_demo_seed_policy` are updated

All feeds currently have `ctl_sync_config = 'Y'`, so a bundle deploy
always restores the table to the CSV baseline.

---

## Quick Reference

| Step | Action | Effect |
| --- | --- | --- |
| **Start** | `UPDATE ops_cfg...` → BACKFILL + aggressive cron | Next cycle uses BACKFILL at accelerated frequency |
| **Wait** | Monitor `DISCOVERED` count | Decreases each cycle |
| **Revert** | `databricks bundle deploy` | `ctl_sync_config=Y` restores INCREMENTAL + original cron from CSV |

---

## Checklist

- [ ] `UPDATE ops_cfg_file_ingestion` → BACKFILL + accelerated `sched_cron` (`force_reprocess=false`)
- [ ] Monitor until `DISCOVERED = 0`
- [ ] `databricks bundle deploy --target {env} -p {profile}` (revert)
- [ ] Verify: all files `LOADED_BRONZE` / `READY_FOR_SILVER`

---

## Onboarding vs Operations

| Purpose | Channel | Persistence |
| --- | --- | --- |
| **Onboarding** (new feeds, schema changes) | CSV in seeds/ → deploy | Permanent until next CSV edit |
| **Operations** (backfill, maintenance) | Direct table UPDATE | Temporal — auto-reverted by next deploy |
| **One-shot** (single targeted run) | CLI `request_json` parameter | Single run only |

---

## Future Cases: Manual One-Shot Trigger

Once the system is caught up, use **principle #15** (job parameter override)
for small targeted backfills — no config or table change needed.

```bash
databricks jobs run-now {job_id} -- \
  --param env={env} \
  --param feed_key={feed_key} \
  --param request_json='{"request_type":"BACKFILL","selector_type":"FILE_DATE","file_date_from":"YYYY-MM-DD","file_date_to":"YYYY-MM-DD","force_reprocess":false}'
```

Re-trigger if `DISCOVERED > 0` after the run completes. Each re-trigger
picks up where the last one left off (`LOADED_BRONZE` files are skipped).

---

## Reference

### Why Files Get Stuck at DISCOVERED

Not a bug — a consequence of the `batch_max_per_run` safety cap.

1. A request discovers N files
2. Batching creates `ceil(N / batch_max_files)` batches
3. If that exceeds `batch_max_per_run`, excess batches are truncated
4. Truncated files stay at `DISCOVERED` and are picked up by subsequent cycles

### Precedence: JSON vs Config Columns

```python
selector_type    = payload.get("selector_type")    or cfg.get("sched_selector_type")
lookback_minutes = payload.get("lookback_minutes") or cfg.get("sched_lookback_minutes", 1440)
```

**The JSON wins.** If `sys_default_request_json` contains a field, the config
column is never reached. Keep both aligned to avoid silent mismatches.

### Potential Future Improvement: Unified Override Surface

Currently, `sys_default_request_json` overrides **what** the dispatcher does
while `sched_cron` is a separate column controlling **when** it fires. These
are updated together in Step 1 as two SET clauses.

A future option is to allow `sched_cron` as a field inside the JSON, following
the same precedence pattern (`payload.get("sched_cron") or cfg.get("sched_cron")`).
This would consolidate operational overrides into a single column — one UPDATE,
one auto-revert on deploy.

**Deferred:** The current two-column approach is explicit and working. Revisit
if the override surface grows to include more scheduling/runtime parameters.

### Error Handling

| `load_status` | Next run behavior |
| --- | --- |
| `LOADED_BRONZE` | Skipped (already done) |
| `DISCOVERED` | Re-selected and re-batched |
| `FAILED` | Retried automatically (principle #12) |
| `STARTED` (within self-heal window) | Blocked until window expires (principle #13) |
| `STARTED` (past self-heal window) | Unblocked, retried automatically |

### Adjudication During Multi-Cycle Backfill

Finalize runs every cycle. `promote_status` evolves as parts get loaded:
- Partial delivery group → `NOT_READY` until all parts are loaded
- Complete delivery group → `READY_FOR_SILVER`
