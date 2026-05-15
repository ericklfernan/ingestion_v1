from __future__ import annotations

"""
Runtime catalog/schema names are keyed by the same `env` value as the Databricks bundle:

- databricks.yml → `variables.env.default` (and per-target overrides) resolve to job parameter defaults via `${var.env}`.
- Job YAML passes `env` into notebooks; widgets must default to the same value for ad-hoc runs.
- ``feed_key`` is **required** at runtime — notebooks raise ``ValueError`` if it is empty.
  ``DEFAULT_FEED_KEY`` is only used as the notebook widget default for ad-hoc interactive runs
  (where the user sees the widget and can change it before executing).

Keep `DEFAULT_ENV` aligned with `variables.env.default` in databricks.yml.

Config on disk (deployed with the bundle) uses an explicit folder layout:

- ``seeds/config/*.csv`` — dispatcher reads every CSV here and syncs rows into the Delta config table.
- ``seeds/source/`` — optional sample files; copied into each feed's UC source folder only when
  ``copy_demo_seed_files`` is true for that env (repo defaults keep it false; set true in a sandbox env if you want seeded samples).

UC layout: ``bronze_schema_name`` is the combined **ops + bronze** schema (config, inventory, ops logs, volumes, and bronze feed tables share this schema). ``silver_schema_name`` / ``gold_schema_name`` are separate schemas for future medallion steps; the current job only writes bronze. Empty silver/gold tables are still provisioned per feed as placeholders (see ``pipeline_services.ensure_feed_environment``).

The ``config_file_name`` key is reserved (not read by runtime). For dispatcher ``run_now``: use ``ingestion_job_name`` matching the deployed Databricks job name (e.g. ``vendor_ingestion_job``); the dispatcher resolves the numeric id at runtime. Optionally set ``ingestion_job_id`` to override and skip name resolution (e.g. duplicate job names or no ``databricks-sdk`` on cluster).

Dispatcher automation policy is centralized here per env:
- ``dispatcher_enable_auto_trigger_runs``: global env kill-switch for dispatcher fan-out.
- ``dispatcher_honor_config_auto_trigger``: whether to honor row-level ``ctl_auto_trigger``.
- ``dispatcher_honor_maintenance_hold``: whether to honor row-level ``ctl_maintenance_hold_until``.
- ``require_src_uri``: when true, each active feed must specify ``src_uri``.
- ``ingestion_test_sleep_seconds``: optional non-prod test delay in ingest_batch to simulate long-running processing.
- ``notification_override_recipients``: when set, all notifications route to this address instead of the feed-level ``notify_recipients`` from config CSV. Supports comma-separated values for multiple recipients or distribution lists (e.g. ``"dev@aetna.com,qa-dl@aetna.com"``). Set in dev/test to avoid spamming prod DLs; leave ``None`` in prod.
"""

# Must match databricks.yml → variables.env.default (and targets.*.variables.env.default when set).
DEFAULT_ENV = "dev"

# Widget default for ad-hoc interactive notebook runs only.
# Notebooks will raise ValueError if feed_key resolves to empty at runtime.
DEFAULT_FEED_KEY = "retro_status_report_ci_aca"


# Single source for catalog/schema/table short names keyed by bundle/job parameter `env`.
ENVIRONMENTS: dict[str, dict[str, str | int | bool | None]] = {
    "dev": {
        "catalog_name": "hcb_dev",
        # Ops (config, inventory, logs) + bronze feed tables + managed volumes live in this schema.
        "bronze_schema_name": "ri_ops_ra_bronze",
        # Silver/gold: separate schemas (naming aligned with ri_ops_ra_*); adjust if UC differs.
        "silver_schema_name": "ri_ops_ra_silver",
        "gold_schema_name": "ri_ops_ra_gold",
        "config_table_name": "ops_cfg_file_ingestion",
        "inventory_table_name": "ops_file_inventory",
        # Placeholder: not read by code today; primary seed path is all ``seeds/config/*.csv`` via dispatcher.
        "config_file_name": "ingestion_config.csv",
        # Deployed job name; dispatcher resolves to numeric id for config-driven run_now dispatches.
        "ingestion_job_name": "vendor_ingestion_job",
        # Optional override (integer). If None, id is resolved from ingestion_job_name via databricks-sdk.
        "ingestion_job_id": None,
        "copy_demo_seed_files": False,
        # Dispatcher activation controls (env-level policy over config rows).
        "dispatcher_enable_auto_trigger_runs": True,
        "dispatcher_honor_config_auto_trigger": True,
        "dispatcher_honor_maintenance_hold": True,
        "require_src_uri": False,
        "ingestion_test_sleep_seconds": 0,
        # When set, overrides per-feed notify_recipients for this env.
        "notification_override_recipients": "roderick.fernan2@aetna.com",
    },
    "test": {
        "catalog_name": "hcb_test",
        "bronze_schema_name": "ri_ops_ra_bronze",
        "silver_schema_name": "ri_ops_ra_silver",
        "gold_schema_name": "ri_ops_ra_gold",
        "config_table_name": "ops_cfg_file_ingestion",
        "inventory_table_name": "ops_file_inventory",
        "config_file_name": "ingestion_config.csv",
        "ingestion_job_name": "vendor_ingestion_job",
        "ingestion_job_id": None,
        "copy_demo_seed_files": False,
        "dispatcher_enable_auto_trigger_runs": True,
        "dispatcher_honor_config_auto_trigger": True,
        "dispatcher_honor_maintenance_hold": True,
        "require_src_uri": False,
        "ingestion_test_sleep_seconds": 360,
        "notification_override_recipients": "roderick.fernan2@aetna.com",
    },
    "prod": {
        "catalog_name": "hcb_prod",
        "bronze_schema_name": "ri_ops_ra_bronze",
        "silver_schema_name": "ri_ops_ra_silver",
        "gold_schema_name": "ri_ops_ra_gold",
        "config_table_name": "ops_cfg_file_ingestion",
        "inventory_table_name": "ops_file_inventory",
        "config_file_name": "ingestion_config.csv",
        "ingestion_job_name": "vendor_ingestion_job",
        "ingestion_job_id": None,
        "copy_demo_seed_files": False,
        "dispatcher_enable_auto_trigger_runs": True,
        "dispatcher_honor_config_auto_trigger": True,
        "dispatcher_honor_maintenance_hold": True,
        "require_src_uri": True,
        "ingestion_test_sleep_seconds": 0,
        "notification_override_recipients": None,
    },
}


def resolve_runtime_settings(env: str | None) -> dict[str, str | int | bool | None]:
    """Resolve settings from job/widget `env`; empty/whitespace uses DEFAULT_ENV (same as bundle default)."""
    key = (env if env and str(env).strip() else DEFAULT_ENV).strip().lower()
    if key not in ENVIRONMENTS:
        raise ValueError(f"Unknown env={env!r}; allowed: {sorted(ENVIRONMENTS)} (bundle var `env` must be one of these)")
    out = dict(ENVIRONMENTS[key])
    out["_env"] = key
    return out
