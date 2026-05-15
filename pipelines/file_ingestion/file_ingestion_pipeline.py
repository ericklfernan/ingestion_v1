"""
File Ingestion Pipeline
=======================

Read this file first. It documents the flow and re-exports the entry points.

Flow
----
1. ORCHESTRATE  (005_dispatcher notebook)
   - scan_config: read CSVs from seeds/config, dedup, sync to Delta config table
   - evaluate_schedule: check cron cooldown + per-row auto-trigger eligibility
   - dispatch_feeds: provision new feeds, trigger ingestion jobs for eligible feeds

2. DISCOVER  (001_request_intake notebook)
   - parse request payload (INCREMENTAL, ADHOC, BACKFILL, DISCOVERY)
   - list source files from vendor drop zone
   - classify paths: pattern match, existence, inventory, blocking, dedup
   - build processing batches within size/count limits

3. MANIFEST  (002_manifest notebook)
   - merge discovered files into ops_file_inventory
   - log discovery results to ops_file_log + ops_discovery_log

4. INGEST  (003_ingest_batch notebook)
   - per-file: read CSV/ZIP, detect schema drift, write to bronze Delta table
   - error-resilient: FAILED status on error (file retryable), deferred writes
   - schema_change_log written on success, notifications on drift

5. FINALIZE  (004_finalize notebook)
   - adjudicate inventory: rank versions, check delivery completeness
   - mark files as READY_FOR_SILVER or NOT_READY
   - log adjudication results

Tables Written
--------------
- ops_cfg_file_ingestion  (config)
- ops_file_inventory      (inventory)
- ops_job_log             (job-level audit)
- ops_file_log            (file-level audit)
- ops_file_schema_change_log
- ops_request_log
- ops_discovery_log
- ops_notifications
- {tgt_bronze_table}     (per-feed bronze Delta table)
"""

# Re-export entry points for notebook imports
from pipelines.file_ingestion.orchestrate.dispatch_feeds import run_dispatcher
from pipelines.file_ingestion.discover.run_request_intake import run_request_intake
from pipelines.file_ingestion.manifest.build_manifest import run_manifest
from pipelines.file_ingestion.ingest.write_to_bronze import run_ingest_batch
from pipelines.file_ingestion.finalize.close_and_summarize import run_finalize, run_rollback_cleanup

__all__ = [
    "run_dispatcher",
    "run_request_intake",
    "run_manifest",
    "run_ingest_batch",
    "run_finalize",
    "run_rollback_cleanup",
]
