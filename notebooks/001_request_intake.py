# Databricks notebook source
import json
import os
import sys

cwd = os.getcwd()
repo_root = cwd if os.path.isdir(os.path.join(cwd, "src")) else os.path.dirname(cwd)
for p in [os.path.join(repo_root, "src"), repo_root]:
    if p not in sys.path:
        sys.path.insert(0, p)

from framework.settings.environment import DEFAULT_ENV, DEFAULT_FEED_KEY, resolve_runtime_settings
from pipelines.file_ingestion.file_ingestion_pipeline import run_request_intake

dbutils.widgets.text("env", DEFAULT_ENV)
dbutils.widgets.text("feed_key", DEFAULT_FEED_KEY)
dbutils.widgets.text("request_json", "")
dbutils.widgets.text("dispatch_run_id", "")
dbutils.widgets.text("batch_max_per_run", "20")

rt = resolve_runtime_settings(dbutils.widgets.get("env"))

_dispatch = dbutils.widgets.get("dispatch_run_id").strip() or None
feed_key = dbutils.widgets.get("feed_key").strip()
if not feed_key:
    raise ValueError("feed_key is required — provide it via job parameter or notebook widget. Do not run without specifying a feed.")

result = run_request_intake(
    dbutils=dbutils,
    spark=spark,
    catalog_name=str(rt["catalog_name"]),
    bronze_schema_name=str(rt["bronze_schema_name"]),
    config_table_name=str(rt["config_table_name"]),
    inventory_table_name=str(rt["inventory_table_name"]),
    feed_key=feed_key,
    request_json=dbutils.widgets.get("request_json"),
    dispatch_run_id=_dispatch,
    batch_max_per_run=int(dbutils.widgets.get("batch_max_per_run") or 20),
    runtime_settings=rt,
)
print(result)
dbutils.notebook.exit(json.dumps(result))
