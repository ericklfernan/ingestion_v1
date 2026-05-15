"""Request intake: discover source files, classify eligibility, build batches."""
from __future__ import annotations

import json
from pathlib import PurePosixPath

from pyspark.sql import functions as F

from framework.settings.feed_config import validate_required, source_dir_request_prefix
from framework.helpers.fingerprint import enrich_source_entry
from framework.helpers.filename_parser import parse_filename_metadata
from framework.helpers.sql_helpers import write_rows
from framework.schemas import request_log_schema, job_log_schema, file_log_schema, discovery_log_schema, notification_schema
from framework.tracking.table_names import core_tables
from framework.tracking.records import make_job_log_record, make_file_log_record, get_task_value
from framework.notifications.notify import make_notification, resolve_recipients
from framework.notifications.constants import SEVERITY_INFO, CAT_INGESTION, EVT_NO_ELIGIBLE_FILES
from framework.provision.provision_feed import load_cfg_and_paths

from .filter_eligible_files import (
    parse_request_payload, select_paths_from_request,
    find_already_done_paths, find_blocked_recent_paths,
    classify_request_paths, build_batch_inputs, build_request_log_record,
)


def run_request_intake(
    dbutils, spark, catalog_name, bronze_schema_name,
    config_table_name, inventory_table_name, feed_key,
    request_json, dispatch_run_id=None, batch_max_per_run=20, runtime_settings=None,
) -> dict:
    validate_required({"catalog_name": catalog_name, "bronze_schema_name": bronze_schema_name, "config_table_name": config_table_name, "inventory_table_name": inventory_table_name, "feed_key": feed_key}, "request intake params")
    resolved = load_cfg_and_paths(spark, catalog_name, bronze_schema_name, bronze_schema_name, bronze_schema_name, config_table_name, inventory_table_name, feed_key, runtime_settings=runtime_settings)
    cfg = resolved["cfg"]
    tables = core_tables(catalog_name, bronze_schema_name)

    source_entries_raw = dbutils.fs.ls(resolved["source_dir"])
    source_entries = [enrich_source_entry({"name": x.name, "file_name": x.name, "file_path": x.path, "size": x.size, "modificationTime": getattr(x, "modificationTime", None)}) for x in source_entries_raw if not x.path.endswith("/")]

    payload = parse_request_payload(request_json or "")
    requested_paths = select_paths_from_request(payload, source_entries, cfg, source_dir_request_prefix(resolved["source_dir"]))

    inventory_paths, already_done_fingerprints = set(), set()
    if spark.catalog.tableExists(resolved["inventory_table"]) and requested_paths:
        inv_tbl = spark.table(resolved["inventory_table"])
        inv_sel = ["file_path", "load_status"]
        if "file_fingerprint" in inv_tbl.columns:
            inv_sel.append("file_fingerprint")
        inv_rows = [r.asDict() for r in inv_tbl.filter(F.col("file_path").isin(requested_paths)).select(*inv_sel).collect()]
        inventory_paths = {r["file_path"] for r in inv_rows}
        already_done_fingerprints = set(find_already_done_paths(inv_rows))

    blocked_recent_fingerprints = set()
    if spark.catalog.tableExists(tables["file_log"]) and requested_paths:
        fl_cols = spark.table(tables["file_log"]).columns
        fl_sel = ["file_path", "stage_name", "stage_status", "ts_event"]
        if "file_fingerprint" in fl_cols:
            fl_sel.append("file_fingerprint")
        fl_rows = [r.asDict() for r in spark.table(tables["file_log"]).filter(F.col("file_path").isin(requested_paths)).filter(F.col("stage_name") == "INGEST_CONTROL").filter(F.col("ts_event") >= F.current_timestamp() - F.expr("INTERVAL 48 HOURS")).select(*fl_sel).collect()]
        blocked_recent_fingerprints = set(find_blocked_recent_paths(fl_rows))

    waterfall = classify_request_paths(requested_paths=requested_paths, source_entries=source_entries, cfg=cfg, inventory_paths=inventory_paths, blocked_recent_fingerprints=blocked_recent_fingerprints, already_done_fingerprints=already_done_fingerprints, force_reprocess=payload.get("force_reprocess", False))

    batch_inputs = [] if payload["request_type"] == "DISCOVERY" else build_batch_inputs(waterfall["arr_paths_final_eligible"], source_entries, cfg)
    total_batches = len(batch_inputs)
    if batch_max_per_run and batch_max_per_run > 0 and len(batch_inputs) > batch_max_per_run:
        batch_inputs = batch_inputs[:batch_max_per_run]
    drid = str(dispatch_run_id).strip() if (dispatch_run_id is not None and str(dispatch_run_id).strip()) else None
    request_record = build_request_log_record(feed_key, payload, waterfall, batch_inputs, dispatch_run_id=drid)
    write_rows(spark, tables["request_log"], [request_record], request_log_schema())

    request_id = request_record["request_id"]
    write_rows(spark, tables["job_log"], [make_job_log_record("request_intake", "SUCCEEDED", feed_key, request_id, request_record["request_status_struct"]["status_code"], dispatch_run_id=drid)], job_log_schema())

    path_to_entry = {str(e["file_path"]): e for e in source_entries}
    file_log_rows = []
    for path in request_record["arr_paths_final_eligible"]:
        file_name = PurePosixPath(path.replace("dbfs:", "")).name
        parsed = parse_filename_metadata(file_name, cfg)
        fp = (path_to_entry.get(path) or {}).get("file_fingerprint")
        file_log_rows.append(make_file_log_record(path, file_name, feed_key, "REQUEST_VALIDATION", "ELIGIBLE", parsed["vendor_code"], parsed["lob_code"], parsed["file_date"], parsed["file_part_seq"], parsed["file_part_tot"], parsed["file_version_label"], parsed["file_version_rank"], parsed["file_extension"], "request path is eligible", None, request_id, fp, dispatch_run_id=drid))
    for path in sorted(set(request_record["arr_paths_final_rejected"])):
        file_name = PurePosixPath(path.replace("dbfs:", "")).name
        parsed = parse_filename_metadata(file_name, cfg)
        reason = "request path is rejected"
        if path in request_record["arr_paths_blocked_recent"]:
            reason = "blocked by recent in-progress status"
        elif path in request_record["arr_paths_already_done"]:
            reason = "already bronze loaded; use force_reprocess to override"
        fp = (path_to_entry.get(path) or {}).get("file_fingerprint")
        file_log_rows.append(make_file_log_record(path, file_name, feed_key, "REQUEST_VALIDATION", "REJECTED", parsed["vendor_code"], parsed["lob_code"], parsed["file_date"], parsed["file_part_seq"], parsed["file_part_tot"], parsed["file_version_label"], parsed["file_version_rank"], parsed["file_extension"], reason, None, request_id, fp, dispatch_run_id=drid))
    write_rows(spark, tables["file_log"], file_log_rows, file_log_schema())

    if payload["request_type"] == "DISCOVERY":
        disc_rows = []
        for path in request_record["arr_paths_requested"]:
            file_name = PurePosixPath(path.replace("dbfs:", "")).name
            disc_rows.append({"event_id": __import__("uuid").uuid4().hex, "request_id": request_id, "feed_key": feed_key, "file_path": path, "file_fingerprint": (path_to_entry.get(path) or {}).get("file_fingerprint") or "", "file_name": file_name, "request_payload_json": json.dumps(payload), "status_reason": "DISCOVERY scaffold only; no bronze ingest performed", "dispatch_run_id": drid, "ts_event": __import__("datetime").datetime.now(__import__("datetime").UTC)})
        write_rows(spark, tables["discovery_log"], disc_rows, discovery_log_schema())

    try:
        dbutils.jobs.taskValues.set(key="request_id", value=request_id)
        dbutils.jobs.taskValues.set(key="batch_inputs", value=batch_inputs)
        dbutils.jobs.taskValues.set(key="eligible_file_paths", value=request_record["arr_paths_final_eligible"])
        dbutils.jobs.taskValues.set(key="request_type", value=payload["request_type"])
        dbutils.jobs.taskValues.set(key="dispatch_run_id", value=drid or "")
        dbutils.jobs.taskValues.set(key="has_eligible_files", value=str(len(batch_inputs) > 0 and payload["request_type"] != "DISCOVERY").lower())
        if not batch_inputs and payload["request_type"] != "DISCOVERY":
            write_rows(spark, tables["notifications"], [make_notification(SEVERITY_INFO, CAT_INGESTION, EVT_NO_ELIGIBLE_FILES, f"No eligible files for {feed_key}", feed_key=feed_key, dispatch_run_id=drid, request_id=request_id, details={"requested_count": len(request_record["arr_paths_requested"]), "rejected_count": len(request_record["arr_paths_final_rejected"])}, resolved_recipients=resolve_recipients(cfg.get("notify_recipients"), (runtime_settings or {}).get("notification_override_recipients")))], notification_schema())
    except Exception:
        pass

    return {"task": "request_intake", "status": "OK", "feed_key": feed_key, "dispatch_run_id": drid, "request_id": request_id, "request_type": request_record["request_type"], "force_reprocess": payload.get("force_reprocess", False), "requested_path_count": len(request_record["arr_paths_requested"]), "blocked_recent_count": len(request_record["arr_paths_blocked_recent"]), "already_done_count": len(request_record["arr_paths_already_done"]), "final_eligible_path_count": len(request_record["arr_paths_final_eligible"]), "final_rejected_path_count": len(request_record["arr_paths_final_rejected"]), "batch_count": len(batch_inputs), "request_log_table": tables["request_log"], "job_log_table": tables["job_log"], "file_log_table": tables["file_log"], "discovery_log_table": tables["discovery_log"], "message": "request intake completed"}
