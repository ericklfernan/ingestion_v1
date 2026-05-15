# Databricks notebook source
import json
import os
import sys

cwd = os.getcwd()
repo_root = cwd if os.path.isdir(os.path.join(cwd, "src")) else os.path.dirname(cwd)
for p in [os.path.join(repo_root, "src"), repo_root]:
    if p not in sys.path:
        sys.path.insert(0, p)

from framework.settings.environment import DEFAULT_ENV
from pipelines.file_ingestion.file_ingestion_pipeline import run_dispatcher

dbutils.widgets.text("env", DEFAULT_ENV)

result = run_dispatcher(
    dbutils=dbutils,
    spark=spark,
    cwd=cwd,
    env=dbutils.widgets.get("env"),
)
print(result)
dbutils.notebook.exit(json.dumps(result))
