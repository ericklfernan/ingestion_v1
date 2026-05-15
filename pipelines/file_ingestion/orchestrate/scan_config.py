"""Config scanning: read CSVs from disk, deduplicate, sync to Delta."""
from __future__ import annotations

import csv
from pathlib import Path

from framework.constants import CONFIG_COLUMNS


def _defaults_for_csv_row() -> dict[str, str]:
    return {
        "feed_sub_key": "DEFAULT", "ctl_active": "Y", "ctl_auto_trigger": "Y",
        "ctl_sync_config": "N", "ctl_maintenance_hold_until": "",
        "src_subdir": "source", "src_uri": "", "ctl_demo_seed_policy": "AUTO",
        "dir_request": "request", "dir_temp": "temp", "dir_discovery": "discovery",
        "src_file_delimiter": "|", "src_file_has_header": "Y",
        "batch_max_files": "10", "batch_max_size_gb": "1.0",
        "sched_selector_type": "FILE_MODIFIED_TS", "sched_lookback_minutes": "1440",
        "sched_cron": "", "sched_timezone": "UTC",
        "sys_default_request_json": "", "schema_read_policy": "FIRST_FILE",
    }


def csv_row_to_delta_dict(raw: dict, config_source_file: str) -> dict:
    d = _defaults_for_csv_row()
    out: dict = {}
    for col, typ in CONFIG_COLUMNS:
        v = raw.get(col)
        if v is None or (isinstance(v, str) and not str(v).strip()):
            v = d.get(col)
        if typ == "INT":
            out[col] = int(v) if v not in (None, "") else int(str(d.get(col, "0") or "0"))
        elif typ == "DOUBLE":
            out[col] = float(v) if v not in (None, "") else float(str(d.get(col, "0") or "0"))
        else:
            out[col] = "" if v is None else str(v).strip()
    if not out.get("tgt_silver_table"):
        out["tgt_silver_table"] = out["tgt_bronze_table"]
    if not out.get("tgt_gold_table"):
        out["tgt_gold_table"] = out["tgt_bronze_table"]
    if not out.get("tgt_volume"):
        out["tgt_volume"] = out["tgt_bronze_table"]
    out["config_source_file"] = config_source_file
    return out


def collect_config_rows_from_disk(config_dir: str) -> list[dict]:
    path = Path(config_dir)
    if not path.is_dir():
        raise FileNotFoundError(f"Config directory not found: {config_dir}")
    rows: list[dict] = []
    for csv_path in sorted(path.glob("*.csv"), key=lambda p: p.stat().st_mtime):
        file_mtime = csv_path.stat().st_mtime
        with csv_path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                continue
            for raw in reader:
                if not str(raw.get("feed_key", "")).strip():
                    continue
                row = csv_row_to_delta_dict(raw, csv_path.name)
                row["config_source_mtime"] = file_mtime
                rows.append(row)
    return rows


def deduplicate_config_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    seen: dict[tuple[str, str], dict] = {}
    dropped: list[dict] = []
    for row in rows:
        k = (str(row.get("feed_key") or "").strip(), str(row.get("feed_sub_key") or "DEFAULT").strip())
        current_file = str(row.get("config_source_file") or "")
        current_mtime = row.get("config_source_mtime", 0)
        if k not in seen:
            seen[k] = row
        else:
            existing = seen[k]
            existing_file = str(existing.get("config_source_file") or "")
            existing_mtime = existing.get("config_source_mtime", 0)
            if current_mtime > existing_mtime:
                dropped.append({"feed_key": k[0], "feed_sub_key": k[1],
                                "dropped_from": existing_file, "winner_from": current_file,
                                "reason": f"superseded by newer file {current_file}"})
                seen[k] = row
            else:
                dropped.append({"feed_key": k[0], "feed_sub_key": k[1],
                                "dropped_from": current_file, "winner_from": existing_file,
                                "reason": f"duplicate in {'same file' if current_file == existing_file else 'older file'}; earlier row kept"})
    return list(seen.values()), dropped


def config_df_with_uc_source_dir(df, catalog_name: str, bronze_schema_name: str):
    from pyspark.sql import functions as F
    prefix = f"/Volumes/{catalog_name}/{bronze_schema_name}/"
    default_path = F.concat(F.lit(prefix), F.col("tgt_volume"), F.lit("/"), F.col("src_subdir"))
    uri = F.trim(F.coalesce(F.col("src_uri"), F.lit("")))
    return df.withColumn("uc_source_dir", F.when(F.length(uri) > 0, uri).otherwise(default_path))


def merge_sync_config(spark, cfg_table: str, incoming_df) -> None:
    incoming_df.createOrReplaceTempView("_dispatcher_cfg_incoming_for_merge")
    match_keys = {"feed_key", "feed_sub_key"}
    sync_cols = [c for c, _ in CONFIG_COLUMNS if c not in match_keys]
    sync_cols += ["config_source_file", "uc_source_dir"]
    full_update = ",\n          ".join(f"t.{c} = s.{c}" for c in sync_cols)
    minimal_update = ",\n          ".join(f"t.{c} = s.{c}" for c in ["uc_source_dir", "src_uri", "ctl_demo_seed_policy"])
    spark.sql(f"""
        MERGE INTO {cfg_table} t
        USING _dispatcher_cfg_incoming_for_merge s
        ON t.feed_key = s.feed_key AND t.feed_sub_key = s.feed_sub_key
        WHEN MATCHED AND upper(trim(s.ctl_sync_config)) = 'Y' THEN UPDATE SET
          {full_update}
        WHEN MATCHED THEN UPDATE SET
          {minimal_update}
    """)
