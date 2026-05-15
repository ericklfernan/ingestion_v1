"""Finalize: adjudicate inventory, summarize results. Rollback utility."""
from __future__ import annotations

from framework.settings.feed_config import validate_required, config_table_name as build_config_table_name, inventory_table_name as build_inventory_table_name, tgt_bronze_table as build_tgt_bronze_table, normalize_config_row
from framework.helpers.sql_helpers import write_rows, sql_string_literal
from framework.schemas import job_log_schema, file_log_schema
from framework.tracking.table_names import core_tables
from framework.tracking.records import make_job_log_record, make_file_log_record, get_task_value
from framework.provision.provision_feed import load_cfg_and_paths


def _effective_dispatch_run_id(dbutils, passed):
    if passed is not None and str(passed).strip():
        return str(passed).strip()
    v = get_task_value(dbutils, "request_intake", "dispatch_run_id", "")
    v = str(v).strip() if v is not None else ""
    return v if v else None


def _adjudicate_inventory(spark, table_name, feed_key):
    """Tiered adjudication: FULL (date+seq), DATED (date only), BARE (no date).

    Group keys are never NULL (set by filename_parser.py tiered fallback).
    Part-completion check only applies when file_part_tot IS NOT NULL (Tier FULL).
    Tier DATED and BARE skip part-completion — silver-readiness requires only
    latest version + LOADED_BRONZE + rows > 0.
    """
    fk_safe = feed_key.replace("'", "''")
    spark.sql(f"""
MERGE INTO {table_name} AS tgt
USING (
  WITH scoped AS (
    SELECT * FROM {table_name} WHERE feed_key = '{fk_safe}'
  ),
  ranked AS (
    SELECT file_path, file_fingerprint, delivery_group_key, part_group_key,
           file_part_seq, file_part_tot, load_status, cnt_row_bronze, parse_status,
           ROW_NUMBER() OVER (
             PARTITION BY part_group_key
             ORDER BY COALESCE(file_version_rank, 0) DESC,
                      COALESCE(ts_discovered, TIMESTAMP '1900-01-01 00:00:00') DESC,
                      file_name DESC
           ) AS rn
    FROM scoped
    WHERE parse_status = 'PARSED'
  ),
  delivery_status AS (
    SELECT delivery_group_key,
           MAX(file_part_tot) AS expected_part_count,
           COUNT(DISTINCT CASE WHEN rn = 1 THEN file_part_seq END) AS latest_part_count,
           COUNT(DISTINCT CASE WHEN rn = 1
                                AND load_status = 'LOADED_BRONZE'
                                AND COALESCE(cnt_row_bronze, 0) > 0
                                AND parse_status = 'PARSED'
                           THEN file_part_seq END) AS loaded_part_count
    FROM ranked
    GROUP BY delivery_group_key
  )
  SELECT s.file_fingerprint, s.file_path,

    -- flg_latest: is this the winning version for its part_group_key?
    CASE
      WHEN s.parse_status <> 'PARSED' THEN 'N'
      WHEN r.rn = 1 THEN 'Y'
      WHEN r.rn IS NULL THEN 'N'
      ELSE 'N'
    END AS flg_latest_new,

    -- flg_superseded: was this beaten by a newer version?
    CASE
      WHEN s.parse_status <> 'PARSED' THEN 'N'
      WHEN r.rn = 1 THEN 'N'
      WHEN r.rn IS NULL THEN 'N'
      ELSE 'Y'
    END AS flg_superseded_new,

    -- flg_legit_for_silver: ready for promotion?
    -- Tier FULL (file_part_tot IS NOT NULL): requires all parts present and loaded
    -- Tier DATED/BARE (file_part_tot IS NULL): just latest + loaded + rows > 0
    CASE
      WHEN s.parse_status <> 'PARSED' THEN 'N'
      WHEN r.rn IS NULL OR r.rn <> 1 THEN 'N'
      WHEN s.load_status <> 'LOADED_BRONZE' THEN 'N'
      WHEN COALESCE(s.cnt_row_bronze, 0) <= 0 THEN 'N'
      WHEN s.file_part_tot IS NOT NULL
           AND COALESCE(ds.latest_part_count, 0) < COALESCE(ds.expected_part_count, 0) THEN 'N'
      WHEN s.file_part_tot IS NOT NULL
           AND COALESCE(ds.loaded_part_count, 0) < COALESCE(ds.expected_part_count, 0) THEN 'N'
      ELSE 'Y'
    END AS flg_legit_for_silver_new,

    -- promote_status
    CASE
      WHEN s.parse_status <> 'PARSED' THEN 'NOT_READY'
      WHEN r.rn IS NULL OR r.rn <> 1 THEN 'NOT_READY'
      WHEN s.load_status <> 'LOADED_BRONZE' THEN 'NOT_READY'
      WHEN COALESCE(s.cnt_row_bronze, 0) <= 0 THEN 'NOT_READY'
      WHEN s.file_part_tot IS NOT NULL
           AND COALESCE(ds.latest_part_count, 0) < COALESCE(ds.expected_part_count, 0) THEN 'NOT_READY'
      WHEN s.file_part_tot IS NOT NULL
           AND COALESCE(ds.loaded_part_count, 0) < COALESCE(ds.expected_part_count, 0) THEN 'NOT_READY'
      ELSE 'READY_FOR_SILVER'
    END AS promote_status_new,

    -- status_reason (waterfall)
    CASE
      WHEN s.parse_status <> 'PARSED'
        THEN COALESCE(s.parse_reason, 'filename parse failed')
      WHEN r.rn IS NULL
        THEN 'parse succeeded but not ranked (unexpected)'
      WHEN r.rn <> 1
        THEN 'superseded by newer version'
      WHEN s.load_status <> 'LOADED_BRONZE'
        THEN 'latest but bronze not loaded'
      WHEN COALESCE(s.cnt_row_bronze, 0) <= 0
        THEN 'latest but zero rows loaded'
      WHEN s.file_part_tot IS NOT NULL
           AND COALESCE(ds.latest_part_count, 0) < COALESCE(ds.expected_part_count, 0)
        THEN 'latest but delivery incomplete'
      WHEN s.file_part_tot IS NOT NULL
           AND COALESCE(ds.loaded_part_count, 0) < COALESCE(ds.expected_part_count, 0)
        THEN 'latest but some parts not bronze loaded'
      ELSE 'latest and ready for silver'
    END AS status_reason_new

  FROM scoped s
  LEFT JOIN ranked r ON s.file_fingerprint = r.file_fingerprint
  LEFT JOIN delivery_status ds ON s.delivery_group_key = ds.delivery_group_key
) AS src
ON tgt.file_fingerprint = src.file_fingerprint
WHEN MATCHED THEN UPDATE SET
  tgt.flg_latest = src.flg_latest_new,
  tgt.flg_superseded = src.flg_superseded_new,
  tgt.flg_legit_for_silver = src.flg_legit_for_silver_new,
  tgt.promote_status = src.promote_status_new,
  tgt.status_reason = src.status_reason_new
""")


def run_finalize(
    dbutils, spark, catalog_name, bronze_schema_name,
    config_table_name, inventory_table_name, feed_key,
    dispatch_run_id=None, runtime_settings=None,
) -> dict:
    resolved = load_cfg_and_paths(spark, catalog_name, bronze_schema_name, bronze_schema_name, bronze_schema_name, config_table_name, inventory_table_name, feed_key, runtime_settings=runtime_settings)
    tables = core_tables(catalog_name, bronze_schema_name)
    drid = _effective_dispatch_run_id(dbutils, dispatch_run_id)
    request_id = get_task_value(dbutils, "request_intake", "request_id", "") or None

    _adjudicate_inventory(spark, resolved["inventory_table"], feed_key)
    inventory_df = spark.table(resolved["inventory_table"]).filter(f"feed_key = '{feed_key}'")
    bronze_df = spark.table(resolved["bronze_table"])
    ready_count = inventory_df.filter("promote_status = 'READY_FOR_SILVER'").count()
    write_rows(spark, tables["job_log"], [make_job_log_record("finalize", "SUCCEEDED", feed_key, request_id, f"ready_for_silver_count={ready_count}", dispatch_run_id=drid)], job_log_schema())

    rows = [r.asDict() for r in inventory_df.collect()]
    file_log_rows = [make_file_log_record(r["file_path"], r["file_name"], feed_key, "ADJUDICATION", r["promote_status"], r.get("vendor_code"), r.get("lob_code"), r.get("file_date"), r.get("file_part_seq"), r.get("file_part_tot"), r.get("file_version_label"), r.get("file_version_rank"), r.get("file_extension"), r.get("status_reason"), r.get("cnt_row_bronze"), request_id, r.get("file_fingerprint"), dispatch_run_id=drid) for r in rows]
    write_rows(spark, tables["file_log"], file_log_rows, file_log_schema())

    return {"task": "finalize", "status": "OK", "feed_key": feed_key, "dispatch_run_id": drid, "bronze_table": resolved["bronze_table"], "inventory_table": resolved["inventory_table"], "total_bronze_rows": bronze_df.count(), "inventory_row_count": inventory_df.count(), "latest_file_count": inventory_df.filter("flg_latest = 'Y'").count(), "superseded_file_count": inventory_df.filter("flg_superseded = 'Y'").count(), "legit_file_count": inventory_df.filter("flg_legit_for_silver = 'Y'").count(), "ready_for_silver_count": inventory_df.filter("promote_status = 'READY_FOR_SILVER'").count(), "message": "finalize completed"}


def run_rollback_cleanup(
    dbutils, spark, catalog_name, bronze_schema_name,
    config_table_name, inventory_table_name, feed_key,
    dispatch_run_id=None, request_id=None,
    drop_feed_table=False, drop_volume=False, purge_config_row=False, dry_run=True,
) -> dict:
    validate_required({"catalog_name": catalog_name, "bronze_schema_name": bronze_schema_name, "config_table_name": config_table_name, "inventory_table_name": inventory_table_name, "feed_key": feed_key}, "rollback params")
    cfg_table = build_config_table_name(catalog_name, bronze_schema_name, config_table_name)
    inv_table = build_inventory_table_name(catalog_name, bronze_schema_name, inventory_table_name)
    tables = core_tables(catalog_name, bronze_schema_name)

    cfg_rows = []
    if spark.catalog.tableExists(cfg_table):
        cfg_rows = spark.table(cfg_table).filter(f"feed_key = {sql_string_literal(feed_key)}").limit(1).collect()
    cfg = normalize_config_row(cfg_rows[0].asDict()) if cfg_rows else None
    bronze_table = build_tgt_bronze_table(catalog_name, bronze_schema_name, cfg) if cfg else None

    where_parts = [f"feed_key = {sql_string_literal(feed_key)}"]
    if dispatch_run_id and str(dispatch_run_id).strip():
        where_parts.append(f"dispatch_run_id = {sql_string_literal(str(dispatch_run_id).strip())}")
    if request_id and str(request_id).strip():
        where_parts.append(f"request_id = {sql_string_literal(str(request_id).strip())}")
    where_clause = " AND ".join(where_parts)

    def _count(table_name, where=None):
        if not spark.catalog.tableExists(table_name):
            return 0
        df = spark.table(table_name)
        return int(df.filter(where).count()) if where else int(df.count())

    candidate_paths = []
    if spark.catalog.tableExists(tables["file_log"]):
        candidate_paths = [str(r["file_path"]) for r in spark.table(tables["file_log"]).filter(where_clause).select("file_path").distinct().collect() if r["file_path"] is not None]

    plan = {"job_log_rows": _count(tables["job_log"], where_clause), "file_log_rows": _count(tables["file_log"], where_clause), "request_log_rows": _count(tables["request_log"], where_clause), "discovery_log_rows": _count(tables["discovery_log"], where_clause), "schema_change_log_rows": _count(tables["schema_change_log"], where_clause), "inventory_rows_for_feed": _count(inv_table, f"feed_key = {sql_string_literal(feed_key)}"), "candidate_file_paths": len(candidate_paths), "bronze_rows_by_source_path": 0}
    if bronze_table and spark.catalog.tableExists(bronze_table) and candidate_paths:
        in_list = ", ".join(sql_string_literal(p) for p in candidate_paths)
        plan["bronze_rows_by_source_path"] = _count(bronze_table, f"src_file_path IN ({in_list})")

    actions_executed = []
    if not dry_run:
        for tname in [tables["file_log"], tables["job_log"], tables["request_log"], tables["discovery_log"], tables["schema_change_log"]]:
            if spark.catalog.tableExists(tname):
                spark.sql(f"DELETE FROM {tname} WHERE {where_clause}")
                actions_executed.append(f"DELETE {tname.split('.')[-1]} WHERE {where_clause}")
        if spark.catalog.tableExists(inv_table):
            spark.sql(f"DELETE FROM {inv_table} WHERE feed_key = {sql_string_literal(feed_key)}")
            actions_executed.append(f"DELETE inventory WHERE feed_key={feed_key}")
        if bronze_table and spark.catalog.tableExists(bronze_table) and candidate_paths:
            in_list = ", ".join(sql_string_literal(p) for p in candidate_paths)
            spark.sql(f"DELETE FROM {bronze_table} WHERE src_file_path IN ({in_list})")
            actions_executed.append(f"DELETE bronze rows by {len(candidate_paths)} source paths")
        if drop_feed_table and bronze_table:
            spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")
            actions_executed.append(f"DROP TABLE {bronze_table}")
        if drop_volume and cfg:
            vol = f"{catalog_name}.{bronze_schema_name}.{cfg['tgt_volume']}"
            spark.sql(f"DROP VOLUME IF EXISTS {vol}")
            actions_executed.append(f"DROP VOLUME {vol}")
        if purge_config_row and spark.catalog.tableExists(cfg_table):
            spark.sql(f"DELETE FROM {cfg_table} WHERE feed_key = {sql_string_literal(feed_key)}")
            actions_executed.append(f"DELETE config WHERE feed_key={feed_key}")

    return {"task": "rollback_cleanup", "status": "OK", "dry_run": bool(dry_run), "feed_key": feed_key, "dispatch_run_id": str(dispatch_run_id).strip() if dispatch_run_id else None, "request_id": str(request_id).strip() if request_id else None, "where_clause": where_clause, "bronze_table": bronze_table, "plan": plan, "actions_executed": actions_executed, "message": "dry-run plan computed" if dry_run else "rollback cleanup executed"}
