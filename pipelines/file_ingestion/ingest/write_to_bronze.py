"""Bronze ingestion: read source files, detect schema drift, write to bronze table."""
from __future__ import annotations

import json
import time
from pathlib import PurePosixPath

from pyspark.sql import functions as F

from framework.settings.feed_config import validate_required
from framework.helpers.fingerprint import enrich_source_entry, compute_file_fingerprint
from framework.helpers.filename_parser import parse_filename_metadata
from framework.helpers.zip_handler import build_extract_dirs, extract_zip_text_files, cleanup_extract_dir, local_to_dbfs_path
from framework.helpers.schema_drift import parse_header_columns, compare_columns, summarize_schema_change
from framework.helpers.sql_helpers import write_rows, sql_string_literal
from framework.schemas import job_log_schema, file_log_schema, schema_change_schema, notification_schema
from framework.tracking.table_names import core_tables
from framework.tracking.records import make_job_log_record, make_file_log_record, make_schema_change_record, get_task_value
from framework.notifications.notify import make_notification, resolve_recipients
from framework.notifications.constants import SEVERITY_WARNING, CAT_SCHEMA, EVT_SCHEMA_DRIFT_DETECTED
from framework.constants import BRONZE_TECHNICAL_COLUMNS
from framework.provision.provision_feed import load_cfg_and_paths, bronze_business_columns
from framework.provision.create_tables import ensure_bronze_business_columns


def _effective_dispatch_run_id(dbutils, passed):
    if passed is not None and str(passed).strip():
        return str(passed).strip()
    v = get_task_value(dbutils, "request_intake", "dispatch_run_id", "")
    v = str(v).strip() if v is not None else ""
    return v if v else None


def _fingerprint_for_ingest(dbutils, source_uri, batch_hint):
    if batch_hint and str(batch_hint).strip():
        return str(batch_hint).strip()
    path = source_uri if str(source_uri).startswith("dbfs:/") else f"dbfs:{source_uri}"
    if "/" not in path.replace("dbfs:", ""):
        return compute_file_fingerprint(path, None, None)
    parent, name = path.rsplit("/", 1)
    try:
        for x in dbutils.fs.ls(parent):
            if x.name == name or str(x.path).rstrip("/") == path.rstrip("/"):
                return enrich_source_entry({"name": x.name, "file_name": x.name, "file_path": x.path, "size": x.size, "modificationTime": getattr(x, "modificationTime", None)})["file_fingerprint"]
    except Exception:
        pass
    return compute_file_fingerprint(path, None, None)


def _mark_bronze_loaded(spark, table_name, file_name, file_path_dbfs, file_fingerprint, row_count, request_id=None, dispatch_run_id=None):
    request_id_sql = ("'" + str(request_id).replace("'", "''") + "'") if request_id else "NULL"
    dispatch_id_sql = ("'" + str(dispatch_run_id).replace("'", "''") + "'") if dispatch_run_id else "NULL"
    fp_sql = "'" + str(file_fingerprint).replace("'", "''") + "'"
    spark.sql(f"""
UPDATE {table_name}
SET load_status = 'LOADED_BRONZE', cnt_row_bronze = {row_count},
    request_id = {request_id_sql}, dispatch_run_id = {dispatch_id_sql},
    status_reason = 'bronze load completed'
WHERE file_fingerprint = {fp_sql}
""")


def run_ingest_batch(
    dbutils, spark, catalog_name, bronze_schema_name,
    config_table_name, inventory_table_name, feed_key,
    batch_id, batch_file_paths_json,
    dispatch_run_id=None, runtime_settings=None,
) -> dict:
    validate_required({"catalog_name": catalog_name, "bronze_schema_name": bronze_schema_name, "config_table_name": config_table_name, "inventory_table_name": inventory_table_name, "feed_key": feed_key, "batch_id": batch_id, "batch_file_paths_json": batch_file_paths_json}, "ingest batch params")
    resolved = load_cfg_and_paths(spark, catalog_name, bronze_schema_name, bronze_schema_name, bronze_schema_name, config_table_name, inventory_table_name, feed_key, runtime_settings=runtime_settings)
    cfg = resolved["cfg"]
    tables = core_tables(catalog_name, bronze_schema_name)
    drid = _effective_dispatch_run_id(dbutils, dispatch_run_id)
    request_id = get_task_value(dbutils, "request_intake", "request_id", "") or None
    batch_refs = json.loads(batch_file_paths_json)
    path_fp_pairs = []
    for item in batch_refs:
        if isinstance(item, dict):
            p = item.get("path") or item.get("file_path") or ""
            fp = item.get("fingerprint") or item.get("file_fingerprint")
            path_fp_pairs.append((str(p), str(fp).strip() if fp else None))
        else:
            path_fp_pairs.append((str(item), None))

    sleep_seconds = 0
    if runtime_settings:
        try:
            sleep_seconds = int(runtime_settings.get("ingestion_test_sleep_seconds", 0) or 0)
        except (TypeError, ValueError):
            sleep_seconds = 0
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)

    total_rows_written, processed_files, failed_files = 0, [], []
    for actual_source_file_path, batch_fp_hint in path_fp_pairs:
        actual_source_file_name = PurePosixPath(actual_source_file_path.replace("dbfs:", "")).name
        source_file_dbfs = actual_source_file_path if actual_source_file_path.startswith("dbfs:/") else f"dbfs:{actual_source_file_path}"
        parsed = parse_filename_metadata(actual_source_file_name, cfg)
        src_fp = _fingerprint_for_ingest(dbutils, source_file_dbfs, batch_fp_hint)

        start_row = make_file_log_record(source_file_dbfs, actual_source_file_name, feed_key, "INGEST_CONTROL", "STARTED", parsed["vendor_code"], parsed["lob_code"], parsed["file_date"], parsed["file_part_seq"], parsed["file_part_tot"], parsed["file_version_label"], parsed["file_version_rank"], parsed["file_extension"], f"batch_id={batch_id}; ingest started", None, request_id, src_fp, dispatch_run_id=drid)
        write_rows(spark, tables["file_log"], [start_row], file_log_schema())

        zip_extract_local_dir = None
        try:
            read_targets = [actual_source_file_path]
            zip_extract_dbfs_dir = None
            if parsed["file_extension"] == "zip":
                zip_extract_dbfs_dir, zip_extract_local_dir = build_extract_dirs(catalog_name, bronze_schema_name, cfg["tgt_volume"], cfg["dir_temp"], actual_source_file_name)
                extracted_local_paths = extract_zip_text_files(source_file_dbfs, zip_extract_local_dir)
                read_targets = [local_to_dbfs_path(p) for p in extracted_local_paths]
                zip_row = make_file_log_record(source_file_dbfs, actual_source_file_name, feed_key, "ZIP_EXTRACT", "SUCCEEDED", parsed["vendor_code"], parsed["lob_code"], parsed["file_date"], parsed["file_part_seq"], parsed["file_part_tot"], parsed["file_version_label"], parsed["file_version_rank"], parsed["file_extension"], f"zip extracted to {zip_extract_dbfs_dir}", len(read_targets), request_id, src_fp, dispatch_run_id=drid)
                write_rows(spark, tables["file_log"], [zip_row], file_log_schema())

            header_text = dbutils.fs.head(read_targets[0] if parsed["file_extension"] == "zip" else source_file_dbfs, 1048576)
            has_header = str(cfg["src_file_has_header"]).upper() == "Y"
            source_columns = parse_header_columns(header_text, cfg["src_file_delimiter"], has_header)
            target_cols = bronze_business_columns(spark, resolved["bronze_table"])
            missing_in_file, new_in_file = compare_columns(source_columns, target_cols)
            change_detected, status_reason = summarize_schema_change(missing_in_file, new_in_file, has_header)
            schema_row = make_schema_change_record(source_file_dbfs, actual_source_file_name, feed_key, resolved["bronze_table"], source_columns, target_cols, missing_in_file, new_in_file, change_detected, status_reason, request_id, src_fp, dispatch_run_id=drid)

            df_raw = spark.read.option("header", has_header).option("sep", cfg["src_file_delimiter"]).option("inferSchema", "false").csv(read_targets)
            business_columns = source_columns if source_columns else df_raw.columns
            ensure_bronze_business_columns(spark, resolved["bronze_table"], business_columns)
            from pyspark.sql.functions import current_timestamp, lit
            df_bronze = df_raw.select([F.col(c).cast("string").alias(c) for c in business_columns]).withColumn("feed_key", lit(feed_key)).withColumn("request_id", lit(request_id)).withColumn("dispatch_run_id", lit(drid)).withColumn("src_file_name", lit(actual_source_file_name)).withColumn("src_file_path", lit(actual_source_file_path)).withColumn("src_file_fingerprint", lit(src_fp)).withColumn("ts_ingest", current_timestamp())
            row_count = df_bronze.count()
            total_rows_written += row_count
            writer = df_bronze.write.option("mergeSchema", "true").mode("append")
            partition_cols_str = str(cfg.get("tgt_bronze_partition_cols") or "").strip()
            if partition_cols_str:
                partition_cols = [c.strip() for c in partition_cols_str.split(",") if c.strip()]
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
            for _attempt in range(3):
                try:
                    writer.saveAsTable(resolved["bronze_table"])
                    break
                except Exception as _write_exc:
                    if "DELTA_METADATA_CHANGED" in str(_write_exc) and _attempt < 2:
                        time.sleep(2)
                        continue
                    raise

            _mark_bronze_loaded(spark, resolved["inventory_table"], actual_source_file_name, source_file_dbfs, src_fp, row_count, request_id=request_id, dispatch_run_id=drid)
            write_rows(spark, tables["schema_change_log"], [schema_row], schema_change_schema())
            if change_detected == "Y":
                write_rows(spark, tables["notifications"], [make_notification(SEVERITY_WARNING, CAT_SCHEMA, EVT_SCHEMA_DRIFT_DETECTED, f"Schema drift in {actual_source_file_name} vs {resolved['bronze_table']}: {status_reason}", feed_key=feed_key, dispatch_run_id=drid, request_id=request_id, details={"file_name": actual_source_file_name, "missing_in_file": missing_in_file, "new_in_file": new_in_file, "bronze_table": resolved["bronze_table"]}, resolved_recipients=resolve_recipients(resolved["cfg"].get("notify_recipients"), (runtime_settings or {}).get("notification_override_recipients")))], notification_schema())

            done_row = make_file_log_record(source_file_dbfs, actual_source_file_name, feed_key, "INGEST_CONTROL", "SUCCEEDED", parsed["vendor_code"], parsed["lob_code"], parsed["file_date"], parsed["file_part_seq"], parsed["file_part_tot"], parsed["file_version_label"], parsed["file_version_rank"], parsed["file_extension"], f"batch_id={batch_id}; ingest completed", row_count, request_id, src_fp, dispatch_run_id=drid)
            bronze_row = make_file_log_record(source_file_dbfs, actual_source_file_name, feed_key, "BRONZE_LOAD", "SUCCEEDED", parsed["vendor_code"], parsed["lob_code"], parsed["file_date"], parsed["file_part_seq"], parsed["file_part_tot"], parsed["file_version_label"], parsed["file_version_rank"], parsed["file_extension"], "bronze load succeeded", row_count, request_id, src_fp, dispatch_run_id=drid)
            write_rows(spark, tables["file_log"], [done_row, bronze_row], file_log_schema())
            processed_files.append({"file_name": actual_source_file_name, "row_count_written": row_count})

        except Exception as exc:
            fail_row = make_file_log_record(source_file_dbfs, actual_source_file_name, feed_key, "INGEST_CONTROL", "FAILED", parsed["vendor_code"], parsed["lob_code"], parsed["file_date"], parsed["file_part_seq"], parsed["file_part_tot"], parsed["file_version_label"], parsed["file_version_rank"], parsed["file_extension"], f"batch_id={batch_id}; ingest failed: {exc}", None, request_id, src_fp, dispatch_run_id=drid)
            write_rows(spark, tables["file_log"], [fail_row], file_log_schema())
            failed_files.append({"file_name": actual_source_file_name, "error": str(exc)})
        finally:
            if zip_extract_local_dir:
                cleanup_extract_dir(zip_extract_local_dir)

    batch_status = "SUCCEEDED" if not failed_files else ("PARTIAL" if processed_files else "FAILED")
    job_reason = f"batch_id={batch_id}; file_count={len(path_fp_pairs)}; succeeded={len(processed_files)}; failed={len(failed_files)}; total_rows_written={total_rows_written}"
    write_rows(spark, tables["job_log"], [make_job_log_record("ingest_batch", batch_status, feed_key, request_id, job_reason, dispatch_run_id=drid)], job_log_schema())

    if failed_files and not processed_files:
        raise RuntimeError(f"All {len(failed_files)} file(s) in batch {batch_id} failed: {failed_files[0]['error']}")

    return {"task": "ingest_batch", "status": "OK" if not failed_files else "PARTIAL", "feed_key": feed_key, "dispatch_run_id": drid, "batch_id": batch_id, "file_count": len(path_fp_pairs), "total_rows_written": total_rows_written, "processed_files": processed_files, "failed_files": failed_files, "bronze_table": resolved["bronze_table"], "inventory_table": resolved["inventory_table"], "message": "ingest batch completed" if not failed_files else f"ingest batch completed with {len(failed_files)} failure(s)"}
