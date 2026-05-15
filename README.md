# Vendor ingestion (Databricks bundle)
#
Reference bundle: **feed config on disk** → **dispatcher** syncs to UC and provisions feeds → **ingestion job** loads **bronze** with ops/inventory tables. Silver/gold schemas exist as placeholders; only bronze is written by this pipeline.

## Deploy

`bundle deploy` uploads notebooks, `src/`, `seeds/`, and job definitions to `.bundle/vendor_ingestion/<target>/`. It does **not** create UC objects until jobs run.

```bash
python -m pytest tests/unit -q
databricks bundle validate --target dev -p <profile>
databricks bundle deploy --target dev -p <profile>
```

Align **`databricks.yml`** `variables.env` with **`environment.DEFAULT_ENV`**. The job parameter **`feed_key`** is **required** — notebooks raise `ValueError` if it is empty. `DEFAULT_FEED_KEY` in `environment.py` is only used as the notebook widget default for ad-hoc interactive runs.

## Jobs

| Job | Role |
|-----|------|
| `vendor_ingestion_dispatcher_job` | `005_dispatcher.py` — config sync, provision, optional `run_now` fan-out |
| `vendor_ingestion_job` | `001`–`004` — intake → **check_eligible_files** (condition) → manifest → parallel ingest → finalize. Skips downstream tasks when no eligible files exist. |
| `vendor_ingestion_rollback_job` | `006_rollback_cleanup.py` — optional targeted rollback (`dry_run` default true) |

**Dispatcher cadence** is whatever is in **`resources/jobs/vendor_ingestion_dispatcher_job.yml`** (currently **every 5 minutes UTC**, unpaused — not duplicated here so it cannot drift).

## Seeds

- **`seeds/config/*.csv`** — dispatcher reads all CSVs (duplicate `(feed_key, feed_sub_key)` across files is rejected).
- **`seeds/schema/`** — header-only schema files (one per feed, e.g. `retro_status_report_ci_aca.txt`). Used by `read_schema_policy` to pre-create bronze business columns at provisioning time. Policy modes: **`FIRST_FILE`** (columns added dynamically from first ingested file), **`SEED`** (read seed; error if missing), **`AUTO`** (read seed if exists; skip otherwise).
- **`seeds/source/`** — demo files; copied when env + `demo_seed_policy` allow (see `environment.py`).

## Doc

Single operator guide: **`docs/RUNBOOK.md`**.

Local demo file shapes: **`seeds/scenarios/README.md`**.

## Same file twice (fingerprint-based)

Eligibility and inventory use **`file_fingerprint`** (path + size + mtime), not the path alone. A second incremental run without **`force_reprocess`** usually skips once that fingerprint is **LOADED_BRONZE**. If the vendor replaces the file (new size/mtime), you get a **new** fingerprint and normal discovery/ingest. With **`force_reprocess`**, the same fingerprint can ingest again and bronze may **append** again.
