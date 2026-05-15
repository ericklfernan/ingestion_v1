"""Feed configuration loading and resolution."""
from __future__ import annotations

from pathlib import Path, PurePosixPath
from framework.constants import CONFIG_COLUMNS


def validate_required(mapping: dict, label: str) -> None:
    missing = [k for k, v in mapping.items() if not str(v).strip()]
    if missing:
        raise ValueError(f"Missing required {label}: {', '.join(missing)}")


def locate_seed_root(cwd: str) -> str:
    candidates = [Path(cwd) / "seeds", Path(cwd).parent / "seeds"]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError(f"Could not find seeds folder from cwd={cwd}")


def load_active_config_row(spark, catalog_name: str, bronze_schema_name: str, config_table_name: str, feed_key: str) -> dict:
    table_name = f"{catalog_name}.{bronze_schema_name}.{config_table_name}"
    rows = spark.table(table_name).filter(
        f"feed_key = '{feed_key}' AND ctl_active = 'Y'"
    ).collect()
    if not rows:
        raise ValueError(f"No active config row found in {table_name} for feed_key={feed_key}")
    return normalize_config_row(rows[0].asDict())


def _normalize_schema_read_policy(value) -> str:
    v = str(value or "FIRST_FILE").strip().upper()
    if v not in {"FIRST_FILE", "SEED", "AUTO"}:
        v = "FIRST_FILE"
    return v


def normalize_config_row(row: dict) -> dict:
    ctl_demo_seed_policy = str(row.get("ctl_demo_seed_policy") or "AUTO").strip().upper()
    if ctl_demo_seed_policy not in {"AUTO", "COPY", "SKIP"}:
        ctl_demo_seed_policy = "AUTO"
    ctl_sync_config = str(row.get("ctl_sync_config") or "N").strip().upper()
    if ctl_sync_config not in {"Y", "N"}:
        ctl_sync_config = "N"
    return {
        "feed_key": str(row["feed_key"]),
        "feed_sub_key": str(row.get("feed_sub_key", "DEFAULT")),
        "ctl_active": str(row.get("ctl_active", "Y")),
        "ctl_auto_trigger": str(row.get("ctl_auto_trigger", "Y")),
        "ctl_sync_config": ctl_sync_config,
        "ctl_maintenance_hold_until": str(row.get("ctl_maintenance_hold_until") or ""),
        "src_file_regex": str(row["src_file_regex"]),
        "src_file_capture_spec": str(row["src_file_capture_spec"]),
        "src_subdir": str(row.get("src_subdir", "source")),
        "src_uri": str(row.get("src_uri") or "").strip(),
        "ctl_demo_seed_policy": ctl_demo_seed_policy,
        "dir_request": str(row.get("dir_request", "request")),
        "dir_temp": str(row.get("dir_temp", "temp")),
        "dir_discovery": str(row.get("dir_discovery", "discovery")),
        "src_file_delimiter": str(row.get("src_file_delimiter", "|")),
        "src_file_has_header": str(row.get("src_file_has_header", "Y")),
        "tgt_bronze_table": str(row["tgt_bronze_table"]),
        "tgt_silver_table": str(row.get("tgt_silver_table") or row["tgt_bronze_table"]),
        "tgt_gold_table": str(row.get("tgt_gold_table") or row["tgt_bronze_table"]),
        "tgt_volume": str(row.get("tgt_volume") or row["tgt_bronze_table"]),
        "batch_max_files": int(row.get("batch_max_files") or 10),
        "batch_max_size_gb": float(row.get("batch_max_size_gb") or 1.0),
        "sched_selector_type": str(row.get("sched_selector_type", "FILE_MODIFIED_TS")),
        "sched_lookback_minutes": int(row.get("sched_lookback_minutes") or 1440),
        "sched_cron": str(row.get("sched_cron") or ""),
        "sched_timezone": str(row.get("sched_timezone") or "UTC"),
        "sys_default_request_json": str(row.get("sys_default_request_json") or ""),
        "schema_read_policy": _normalize_schema_read_policy(row.get("schema_read_policy")),
        "tgt_bronze_partition_cols": str(row.get("tgt_bronze_partition_cols") or "").strip(),
    }


def resolve_schema_seed_path(seed_root: str, tgt_bronze_table: str) -> str:
    return str(PurePosixPath(seed_root) / "schema" / f"{tgt_bronze_table}.txt")


# ---------------------------------------------------------------------------
# Table / path name builders
# ---------------------------------------------------------------------------

def config_table_name(catalog_name: str, bronze_schema_name: str, table_name: str) -> str:
    return f"{catalog_name}.{bronze_schema_name}.{table_name}"


def inventory_table_name(catalog_name: str, bronze_schema_name: str, table_name: str) -> str:
    return f"{catalog_name}.{bronze_schema_name}.{table_name}"


def tgt_bronze_table(catalog_name: str, bronze_schema_name: str, cfg: dict) -> str:
    return f"{catalog_name}.{bronze_schema_name}.{cfg['tgt_bronze_table']}"


def tgt_silver_table(catalog_name: str, silver_schema_name: str, cfg: dict) -> str:
    return f"{catalog_name}.{silver_schema_name}.{cfg['tgt_silver_table']}"


def tgt_gold_table(catalog_name: str, gold_schema_name: str, cfg: dict) -> str:
    return f"{catalog_name}.{gold_schema_name}.{cfg['tgt_gold_table']}"


def volume_root(catalog_name: str, bronze_schema_name: str, cfg: dict) -> str:
    return f"/Volumes/{catalog_name}/{bronze_schema_name}/{cfg['tgt_volume']}"


def format_uc_source_dir(catalog_name: str, bronze_schema_name: str, tgt_volume: str, src_subdir: str) -> str:
    return f"/Volumes/{catalog_name}/{bronze_schema_name}/{tgt_volume}/{src_subdir}"


def is_external_vendor_storage_uri(path: str) -> bool:
    p = (path or "").strip().lower()
    return p.startswith(("s3://", "s3a://", "abfss://", "wasbs://", "gs://"))


def resolve_vendor_source_dir(catalog_name: str, bronze_schema_name: str, cfg: dict) -> str:
    override = str(cfg.get("src_uri") or "").strip()
    if override:
        return override.rstrip("/")
    return format_uc_source_dir(catalog_name, bronze_schema_name, cfg["tgt_volume"], cfg["src_subdir"])


def apply_environment_policy(cfg: dict, runtime_settings: dict | None) -> dict:
    if not runtime_settings:
        return cfg
    out = dict(cfg)
    require_src_uri = bool(runtime_settings.get("require_src_uri", False))
    if require_src_uri and not str(out.get("src_uri") or "").strip():
        env_key = str(runtime_settings.get("_env") or "unknown")
        fk = str(out.get("feed_key") or "<unknown>")
        raise ValueError(
            f"env={env_key} requires src_uri for feed_key={fk}; "
            "set src_uri in config CSV/table for this feed"
        )
    return out


def source_dir_request_prefix(source_dir: str) -> str:
    s = (source_dir or "").strip().rstrip("/")
    if not s:
        return s
    if s.startswith("dbfs:"):
        return s.rstrip("/")
    if is_external_vendor_storage_uri(s):
        return s.rstrip("/")
    if s.startswith("/Volumes/"):
        return f"dbfs:{s}".rstrip("/")
    return f"dbfs:{s}".rstrip("/")


def should_copy_demo_seed_files(cfg: dict, default_enabled: bool, is_new_provision: bool) -> bool:
    policy = str(cfg.get("ctl_demo_seed_policy") or "AUTO").strip().upper()
    if policy == "COPY":
        return True
    if policy == "SKIP":
        return False
    return bool(default_enabled and is_new_provision)


def folder_paths(catalog_name: str, bronze_schema_name: str, cfg: dict) -> dict:
    root = volume_root(catalog_name, bronze_schema_name, cfg)
    return {
        "volume_root": root,
        "source_dir": resolve_vendor_source_dir(catalog_name, bronze_schema_name, cfg),
        "request_dir": f"{root}/{cfg['dir_request']}",
        "temp_dir": f"{root}/{cfg['dir_temp']}",
        "discovery_dir": f"{root}/{cfg['dir_discovery']}",
        "config_dir": f"{root}/config",
    }


def create_shared_schema_sql(catalog_name: str, schema_name: str) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}"


def create_volume_sql(catalog_name: str, bronze_schema_name: str, cfg: dict) -> str:
    return f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{bronze_schema_name}.{cfg['tgt_volume']}"


def copy_matching_seed_files(seed_root: str, cfg: dict, target_source_dir: str) -> list[str]:
    import re
    if is_external_vendor_storage_uri(target_source_dir):
        return []
    source_dir = Path(seed_root) / "source"
    if not source_dir.exists():
        return []
    target_dir = Path(target_source_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    copied = []
    for p in sorted(source_dir.iterdir()):
        if not p.is_file():
            continue
        if re.match(cfg["src_file_regex"], p.name, flags=re.IGNORECASE):
            (target_dir / p.name).write_bytes(p.read_bytes())
            copied.append(p.name)
    return copied
