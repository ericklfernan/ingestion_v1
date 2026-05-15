"""Manifest: discover files, merge into inventory, log results."""
from __future__ import annotations

from framework.helpers.fingerprint import enrich_source_entry
from framework.helpers.sql_helpers import write_rows
from framework.schemas import job_log_schema, file_log_schema, inventory_struct_schema
from framework.tracking.table_names import core_tables
from framework.tracking.records import make_job_log_record, make_file_log_record, get_task_value
from framework.provision.provision_feed import load_cfg_and_paths

from framework.constants import INVENTORY_COLUMNS
from framework.helpers.filename_parser import parse_filename_metadata


def discovery_rows(source_entries, cfg, request_id=None, dispatch_run_id=None):
    rows = []
    for entry in source_entries:
        parsed = parse_filename_metadata(entry["file_name"], cfg)
        rows.append({
            "feed_key": cfg["feed_key"], "request_id": request_id,
            "dispatch_run_id": dispatch_run_id, "feed_sub_key": cfg["feed_sub_key"],
            "file_name": entry["file_name"], "file_path": entry["file_path"],
            "file_fingerprint": entry["file_fingerprint"],
            "file_size": entry.get("file_size"), "src_size": entry.get("src_size"),
            "src_mtime_ms": entry.get("src_mtime_ms"),
            "vendor_code": parsed["vendor_code"], "lob_code": parsed["lob_code"],
            "file_date": parsed["file_date"], "file_part_seq": parsed["file_part_seq"],
            "file_part_tot": parsed["file_part_tot"],
            "file_version_label": parsed["file_version_label"],
            "file_version_rank": parsed["file_version_rank"],
            "file_extension": parsed["file_extension"],
            "delivery_group_key": parsed["delivery_group_key"],
            "part_group_key": parsed["part_group_key"],
            "parse_status": parsed["parse_status"], "parse_reason": parsed["parse_reason"],
            "load_status": "DISCOVERED" if parsed["parse_status"] == "PARSED" else "PARSE_FAILED",
            "cnt_row_bronze": None, "flg_latest": "N", "flg_superseded": "N",
            "flg_legit_for_silver": "N", "promote_status": "NOT_READY",
            "status_reason": parsed["parse_reason"] or "discovered",
        })
    return rows


def merge_discovery_rows(spark, table_name, rows):
    if not rows:
        return
    import uuid
    from pyspark.sql import functions as F_
    temp_view = f"tmp_discovery_{uuid.uuid4().hex[:8]}"
    df = spark.createDataFrame(rows, schema=inventory_struct_schema())
    df.withColumn("ts_discovered", F_.current_timestamp()).createOrReplaceTempView(temp_view)
    spark.sql(f"""
MERGE INTO {table_name} AS tgt
USING {temp_view} AS src
ON tgt.file_fingerprint = src.file_fingerprint
WHEN MATCHED THEN UPDATE SET
  {', '.join(f'tgt.{n} = src.{n}' for n, _ in INVENTORY_COLUMNS if n not in ('load_status', 'cnt_row_bronze', 'flg_latest', 'flg_superseded', 'flg_legit_for_silver', 'promote_status', 'ts_discovered', 'status_reason'))},
  tgt.ts_discovered = src.ts_discovered, tgt.status_reason = src.status_reason
WHEN NOT MATCHED THEN INSERT *
""")


def run_manifest(
    dbutils, spark, catalog_name, bronze_schema_name,
    config_table_name, inventory_table_name, feed_key,
    dispatch_run_id=None, runtime_settings=None,
) -> dict:
    resolved = load_cfg_and_paths(spark, catalog_name, bronze_schema_name, bronze_schema_name, bronze_schema_name, config_table_name, inventory_table_name, feed_key, runtime_settings=runtime_settings)
    tables = core_tables(catalog_name, bronze_schema_name)
    drid = _effective_dispatch_run_id(dbutils, dispatch_run_id)
    request_id = get_task_value(dbutils, "request_intake", "request_id", "")
    request_type = get_task_value(dbutils, "request_intake", "request_type", "")
    eligible_file_paths = get_task_value(dbutils, "request_intake", "eligible_file_paths", [])
    eligible_file_paths = set(eligible_file_paths if isinstance(eligible_file_paths, list) else [])

    entries = dbutils.fs.ls(resolved["source_dir"])
    file_entries = [x for x in entries if not x.path.endswith("/")]
    filtered_entries = [x for x in file_entries if x.path in eligible_file_paths] if eligible_file_paths else []

    listing = [enrich_source_entry({"name": x.name, "file_name": x.name, "file_path": x.path, "size": x.size, "modificationTime": getattr(x, "modificationTime", None)}) for x in filtered_entries]
    rows = discovery_rows(listing, resolved["cfg"], request_id=request_id or None, dispatch_run_id=drid)
    merge_discovery_rows(spark, resolved["inventory_table"], rows)

    write_rows(spark, tables["job_log"], [make_job_log_record("manifest", "SUCCEEDED", feed_key, request_id or None, f"discovered_file_count={len(filtered_entries)}; request_type={request_type}", dispatch_run_id=drid)], job_log_schema())
    file_log_rows = [make_file_log_record(r["file_path"], r["file_name"], feed_key, "DISCOVERY", "SUCCEEDED", r["vendor_code"], r["lob_code"], r["file_date"], r["file_part_seq"], r["file_part_tot"], r["file_version_label"], r["file_version_rank"], r["file_extension"], r["parse_status"], None, request_id or None, r.get("file_fingerprint"), dispatch_run_id=drid) for r in rows]
    write_rows(spark, tables["file_log"], file_log_rows, file_log_schema())

    scoped = spark.table(resolved["inventory_table"]).filter(f"feed_key = '{feed_key}'")
    return {"task": "manifest", "status": "OK", "feed_key": feed_key, "dispatch_run_id": drid, "request_type": request_type, "discovered_file_count": len(filtered_entries), "inventory_row_count": scoped.count(), "parsed_file_count": scoped.filter("parse_status = 'PARSED'").count(), "parse_failed_file_count": scoped.filter("parse_status = 'PARSE_FAILED'").count(), "inventory_table": resolved["inventory_table"], "message": "manifest completed"}


def _effective_dispatch_run_id(dbutils, passed):
    if passed is not None and str(passed).strip():
        return str(passed).strip()
    v = get_task_value(dbutils, "request_intake", "dispatch_run_id", "")
    v = str(v).strip() if v is not None else ""
    return v if v else None
