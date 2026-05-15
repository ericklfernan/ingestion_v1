"""Dispatcher: scan config, sync to Delta, provision feeds, trigger ingestion jobs."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F

from framework.settings.environment import resolve_runtime_settings
from framework.settings.feed_config import (
    locate_seed_root, apply_environment_policy,
    config_table_name as build_config_table_name,
    create_shared_schema_sql,
)
from framework.schemas import job_log_schema, notification_schema, config_delta_struct_type
from framework.tracking.table_names import core_tables
from framework.tracking.records import make_job_log_record
from framework.helpers.sql_helpers import write_rows, sql_string_literal
from framework.notifications.notify import make_notification, resolve_recipients
from framework.notifications.constants import (
    SEVERITY_INFO, SEVERITY_WARNING, SEVERITY_ERROR,
    CAT_DISPATCHER, CAT_CONFIG,
    EVT_DUPLICATE_CONFIG_RESOLVED, EVT_EMPTY_CONFIG,
    EVT_AUTO_TRIGGER_DISABLED, EVT_JOB_RESOLVE_FAILED,
)
from framework.provision.create_tables import ensure_ops_tables
from framework.provision.provision_feed import ensure_feed_environment

from .scan_config import (
    collect_config_rows_from_disk, deduplicate_config_rows,
    config_df_with_uc_source_dir, merge_sync_config,
)
from .evaluate_schedule import should_auto_trigger_row, compute_next_dispatched_at


def _ensure_shared_schemas(spark, catalog_name, bronze_schema_name, silver_schema_name, gold_schema_name):
    spark.sql(create_shared_schema_sql(catalog_name, bronze_schema_name))
    spark.sql(create_shared_schema_sql(catalog_name, silver_schema_name))
    spark.sql(create_shared_schema_sql(catalog_name, gold_schema_name))


def _resolve_ingestion_job_id(rt: dict) -> tuple[int | None, str | None]:
    jid = rt.get("ingestion_job_id")
    if jid is not None and str(jid).strip() != "":
        try:
            return int(jid), None
        except (TypeError, ValueError):
            return None, f"ingestion_job_id must be an integer, got {jid!r}"
    name = str(rt.get("ingestion_job_name") or "").strip()
    if not name:
        return None, "set ingestion_job_name (or numeric ingestion_job_id) in environment.ENVIRONMENTS"
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        return None, "ingestion_job_name is set but databricks-sdk is not importable on this cluster"
    try:
        w = WorkspaceClient()
    except Exception as e:
        return None, f"WorkspaceClient() failed ({e!s})"
    try:
        jobs = list(w.jobs.list())
    except Exception as e:
        return None, f"jobs.list failed: {e!s}"
    for job in jobs:
        settings = getattr(job, "settings", None)
        jname = getattr(settings, "name", None) if settings is not None else None
        if jname == name:
            found = getattr(job, "job_id", None)
            if found is not None:
                return int(found), None
    suffix = f" {name}"
    suffix_matches = []
    for job in jobs:
        settings = getattr(job, "settings", None)
        jname = str(getattr(settings, "name", "") or "") if settings is not None else ""
        if jname.endswith(suffix):
            found = getattr(job, "job_id", None)
            if found is not None:
                suffix_matches.append((int(found), jname))
    if len(suffix_matches) == 1:
        return suffix_matches[0][0], None
    if len(suffix_matches) > 1:
        names = ", ".join([m[1] for m in suffix_matches[:5]])
        return None, f"multiple jobs matched suffix name={name!r}; set ingestion_job_id explicitly. matches: {names}"
    return None, f"no Databricks job found with settings.name={name!r}"


def _try_run_now_ingestion(dbutils, job_id, env, feed_key, request_json, dispatch_run_id=None):
    dr = str(dispatch_run_id).strip() if dispatch_run_id else ""
    params = {"env": env, "feed_key": feed_key, "request_json": request_json, "dispatch_run_id": dr}
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        run = w.jobs.run_now(job_id=job_id, job_parameters=params)
        run_id = getattr(run, "run_id", None)
        if run_id is not None:
            return {"ok": True, "run_id": str(run_id)}
        return {"ok": False, "reason": "run_now returned no run_id"}
    except Exception as sdk_err:
        sdk_reason = str(sdk_err)
    try:
        jobs_attr = getattr(dbutils, "jobs", None)
        run_now = getattr(jobs_attr, "runNow", None) if jobs_attr is not None else None
        if run_now is None:
            return {"ok": False, "reason": f"dbutils.jobs.runNow not available; sdk_error={sdk_reason}"}
        try:
            rid = run_now(job_id=job_id, job_parameters=params)
        except TypeError:
            rid = run_now(job_id, params)
        return {"ok": True, "run_id": str(rid)}
    except Exception as e:
        return {"ok": False, "reason": f"dbutils runNow failed ({e!s}); sdk_error={sdk_reason}"}


def _active_ingestion_feed_keys(job_id: int) -> tuple[set[str], str | None]:
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        return set(), "databricks-sdk not importable for active-run guard"
    try:
        w = WorkspaceClient()
    except Exception as e:
        return set(), f"WorkspaceClient() failed for active-run guard: {e!s}"
    active_keys: set[str] = set()
    try:
        runs = w.jobs.list_runs(job_id=job_id, active_only=True, limit=25)
        for run in runs:
            params = getattr(run, "job_parameters", None)
            fk = None
            if isinstance(params, dict):
                fk = params.get("feed_key")
            elif params is not None:
                try:
                    for p in params:
                        k = getattr(p, "key", None) or getattr(p, "name", None)
                        v = getattr(p, "value", None)
                        if k == "feed_key":
                            fk = v
                            break
                except TypeError:
                    pass
            if fk is not None and str(fk).strip():
                active_keys.add(str(fk).strip())
        return active_keys, None
    except Exception as e:
        return set(), f"list_runs failed: {e!s}"


def run_dispatcher(dbutils, spark, cwd: str, env: str | None) -> dict:
    dispatch_run_id = uuid4().hex
    rt = resolve_runtime_settings(env)
    catalog_name = str(rt["catalog_name"])
    bronze_schema_name = str(rt["bronze_schema_name"])
    silver_schema_name = str(rt["silver_schema_name"])
    gold_schema_name = str(rt["gold_schema_name"])
    config_table_name = str(rt["config_table_name"])
    inventory_table_name_val = str(rt["inventory_table_name"])
    copy_demo_seed_files = bool(rt.get("copy_demo_seed_files"))
    seed_root = locate_seed_root(cwd)
    config_dir = str(Path(seed_root) / "config")
    disk_rows = [apply_environment_policy(r, rt) for r in collect_config_rows_from_disk(config_dir)]

    if not disk_rows:
        _ensure_shared_schemas(spark, catalog_name, bronze_schema_name, silver_schema_name, gold_schema_name)
        tables = ensure_ops_tables(spark, catalog_name, bronze_schema_name, inventory_table_name_val)
        write_rows(spark, tables["job_log"], [make_job_log_record("dispatcher", "SUCCEEDED", "dispatcher", None, "no config rows in seeds/config", dispatch_run_id=dispatch_run_id)], job_log_schema())
        write_rows(spark, tables["notifications"], [make_notification(SEVERITY_WARNING, CAT_CONFIG, EVT_EMPTY_CONFIG, "No config rows found in seeds/config/*.csv", dispatch_run_id=dispatch_run_id, resolved_recipients=resolve_recipients(None, rt.get("notification_override_recipients")))], notification_schema())
        return {"task": "dispatcher", "status": "OK", "env": str(rt["_env"]), "dispatch_run_id": dispatch_run_id, "config_table": build_config_table_name(catalog_name, bronze_schema_name, config_table_name), "sync_note": "no config rows", "provisioned_feed_keys": [], "provisioned": [], "triggered": [], "ingestion_job_id_resolved": None, "ingestion_job_resolve_error": None, "message": "dispatcher completed — no config to process"}

    disk_rows, duplicate_log = deduplicate_config_rows(disk_rows)
    cfg_table = build_config_table_name(catalog_name, bronze_schema_name, config_table_name)
    schema = config_delta_struct_type()
    _ensure_shared_schemas(spark, catalog_name, bronze_schema_name, silver_schema_name, gold_schema_name)

    table_created = not spark.catalog.tableExists(cfg_table)
    incoming_df = spark.createDataFrame(disk_rows, schema=schema)
    incoming_df = config_df_with_uc_source_dir(incoming_df, catalog_name, bronze_schema_name)
    incoming_df = incoming_df.withColumn("dispatch_run_id", F.lit(dispatch_run_id))

    if table_created:
        incoming_df.write.format("delta").mode("overwrite").saveAsTable(cfg_table)
        keys_to_provision = sorted({r["feed_key"] for r in disk_rows if str(r.get("ctl_active", "Y")).upper() == "Y"})
        sync_note = "config table created from disk"
    else:
        existing = spark.table(cfg_table).select("feed_key", "feed_sub_key")
        to_add = incoming_df.join(existing, ["feed_key", "feed_sub_key"], "left_anti")
        n_new = to_add.count()
        if n_new > 0:
            to_add.write.format("delta").mode("append").saveAsTable(cfg_table)
        keys_to_provision = sorted({r["feed_key"] for r in to_add.filter("upper(trim(ctl_active)) = 'Y'").select("feed_key").distinct().collect()})
        sync_note = f"appended {n_new} new config row(s)"
        merge_sync_config(spark, cfg_table, incoming_df)

    tables = ensure_ops_tables(spark, catalog_name, bronze_schema_name, inventory_table_name_val)
    _feed_recipients = {r["feed_key"]: r.get("notify_recipients") for r in disk_rows}
    _env_override = rt.get("notification_override_recipients")

    if duplicate_log:
        dup_warnings = [make_job_log_record("dispatcher_dedup", "WARNING", str(d["feed_key"]), None, f"duplicate {d['feed_key']}|{d['feed_sub_key']}: dropped from {d['dropped_from']}, winner from {d['winner_from']}; {d['reason']}", dispatch_run_id=dispatch_run_id) for d in duplicate_log]
        write_rows(spark, tables["job_log"], dup_warnings, job_log_schema())
        write_rows(spark, tables["notifications"], [make_notification(SEVERITY_WARNING, CAT_CONFIG, EVT_DUPLICATE_CONFIG_RESOLVED, f"Duplicate config resolved: {d['feed_key']}|{d['feed_sub_key']} — winner from {d['winner_from']}", feed_key=d["feed_key"], dispatch_run_id=dispatch_run_id, details=d, resolved_recipients=resolve_recipients(_feed_recipients.get(d["feed_key"]), _env_override)) for d in duplicate_log], notification_schema())

    provisioned = []
    for fk in keys_to_provision:
        try:
            provisioned.append(ensure_feed_environment(dbutils, spark, seed_root, catalog_name, bronze_schema_name, silver_schema_name, gold_schema_name, config_table_name, inventory_table_name_val, fk, copy_demo_seed_files=copy_demo_seed_files, is_new_provision=True, runtime_settings=rt))
        except Exception as e:
            provisioned.append({"feed_key": fk, "status": "ERROR", "error": str(e)})

    tables = core_tables(catalog_name, bronze_schema_name)
    _fk_log = keys_to_provision[0] if keys_to_provision else "dispatcher"
    write_rows(spark, tables["job_log"], [make_job_log_record("dispatcher", "SUCCEEDED", _fk_log, None, sync_note, dispatch_run_id=dispatch_run_id)], job_log_schema())

    triggered = []
    ingestion_job_id_resolved = None
    ingestion_job_resolve_error = None
    auto_trigger_enabled = bool(rt.get("dispatcher_enable_auto_trigger_runs", True))
    honor_config_auto_trigger = bool(rt.get("dispatcher_honor_config_auto_trigger", True))
    honor_maintenance_hold = bool(rt.get("dispatcher_honor_maintenance_hold", True))

    if not auto_trigger_enabled:
        triggered.append({"note": "auto-trigger disabled by environment policy"})
        write_rows(spark, tables["job_log"], [make_job_log_record("dispatcher_trigger", "SKIPPED", "dispatcher", None, "dispatcher_enable_auto_trigger_runs=false", dispatch_run_id=dispatch_run_id)], job_log_schema())
        write_rows(spark, tables["notifications"], [make_notification(SEVERITY_INFO, CAT_DISPATCHER, EVT_AUTO_TRIGGER_DISABLED, "Auto-trigger disabled by environment policy", dispatch_run_id=dispatch_run_id, resolved_recipients=resolve_recipients(None, _env_override))], notification_schema())
    else:
        j_id, resolve_err = _resolve_ingestion_job_id(rt)
        ingestion_job_id_resolved = j_id
        ingestion_job_resolve_error = resolve_err
        if j_id is None:
            triggered.append({"note": resolve_err or "could not resolve ingestion job"})
            write_rows(spark, tables["job_log"], [make_job_log_record("dispatcher_trigger", "FAILED", "dispatcher", None, resolve_err or "could not resolve ingestion job", dispatch_run_id=dispatch_run_id)], job_log_schema())
            write_rows(spark, tables["notifications"], [make_notification(SEVERITY_ERROR, CAT_DISPATCHER, EVT_JOB_RESOLVE_FAILED, f"Cannot resolve ingestion job: {resolve_err}", dispatch_run_id=dispatch_run_id, details={"error": resolve_err}, resolved_recipients=resolve_recipients(None, _env_override))], notification_schema())
        else:
            env_key = str(rt["_env"])
            active_keys, active_check_err = _active_ingestion_feed_keys(j_id)
            if active_check_err:
                write_rows(spark, tables["job_log"], [make_job_log_record("dispatcher_trigger", "WARNING", "dispatcher", None, active_check_err, dispatch_run_id=dispatch_run_id)], job_log_schema())
            # Load dispatch state for schedule evaluation
            dispatch_state = {}
            ds_table = tables["dispatch_state"]
            if spark.catalog.tableExists(ds_table):
                for ds_row in spark.table(ds_table).select("feed_key", "feed_sub_key", "last_dispatched_at").collect():
                    ds_dict = ds_row.asDict()
                    dispatch_state[(str(ds_dict["feed_key"]), str(ds_dict.get("feed_sub_key") or "DEFAULT"))] = ds_dict.get("last_dispatched_at")

            for row in spark.table(cfg_table).collect():
                row_dict = row.asDict()
                # Inject last_dispatched_at from dispatch state (not from config table)
                ds_key = (str(row_dict.get("feed_key") or ""), str(row_dict.get("feed_sub_key") or "DEFAULT"))
                row_dict["last_dispatched_at"] = dispatch_state.get(ds_key)
                if not should_auto_trigger_row(row_dict, honor_config_auto_trigger=honor_config_auto_trigger, honor_maintenance_hold=honor_maintenance_hold):
                    write_rows(spark, tables["job_log"], [make_job_log_record("dispatcher_trigger", "SKIPPED", str(row_dict.get("feed_key") or "unknown"), None, "row not eligible", dispatch_run_id=dispatch_run_id)], job_log_schema())
                    continue
                fk = row_dict["feed_key"]
                if str(fk) in active_keys:
                    reason = f"active ingestion run already exists for feed_key={fk}"
                    triggered.append({"feed_key": fk, "ok": False, "reason": reason, "skipped": True})
                    write_rows(spark, tables["job_log"], [make_job_log_record("dispatcher_trigger", "SKIPPED", str(fk), None, reason, dispatch_run_id=dispatch_run_id)], job_log_schema())
                    continue
                fsk = str(row_dict.get("feed_sub_key") or "DEFAULT")
                req = row_dict.get("sys_default_request_json") or ""
                res = _try_run_now_ingestion(dbutils, j_id, env_key, str(fk), str(req), dispatch_run_id)
                if res.get("ok"):
                    _nxt = compute_next_dispatched_at(str(row_dict.get("sched_cron") or ""))
                    next_expr = f"TIMESTAMP '{_nxt.strftime('%Y-%m-%d %H:%M:%S')}'" if _nxt else "NULL"
                    ds_table = tables["dispatch_state"]
                    spark.sql(f"""
                        MERGE INTO {ds_table} t
                        USING (SELECT {sql_string_literal(str(fk))} AS feed_key, {sql_string_literal(fsk)} AS feed_sub_key) s
                        ON t.feed_key = s.feed_key AND t.feed_sub_key = s.feed_sub_key
                        WHEN MATCHED THEN UPDATE SET
                          last_dispatched_at = current_timestamp(),
                          next_dispatched_at = {next_expr},
                          dispatch_run_id = {sql_string_literal(dispatch_run_id)}
                        WHEN NOT MATCHED THEN INSERT (feed_key, feed_sub_key, last_dispatched_at, next_dispatched_at, dispatch_run_id)
                          VALUES (s.feed_key, s.feed_sub_key, current_timestamp(), {next_expr}, {sql_string_literal(dispatch_run_id)})
                    """)
                triggered.append({"feed_key": fk, **res})
                trigger_status = "SUCCEEDED" if res.get("ok") else "FAILED"
                trigger_reason = f"run_id={res.get('run_id')}" if res.get("ok") else str(res.get("reason") or "run_now failed")
                write_rows(spark, tables["job_log"], [make_job_log_record("dispatcher_trigger", trigger_status, str(fk), None, trigger_reason, dispatch_run_id=dispatch_run_id)], job_log_schema())

    return {"task": "dispatcher", "status": "OK", "env": str(rt["_env"]), "dispatch_run_id": dispatch_run_id, "config_table": cfg_table, "sync_note": sync_note, "provisioned_feed_keys": keys_to_provision, "provisioned": provisioned, "triggered": triggered, "ingestion_job_id_resolved": ingestion_job_id_resolved, "ingestion_job_resolve_error": ingestion_job_resolve_error, "message": "dispatcher completed"}
