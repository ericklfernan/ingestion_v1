"""Feed-level provisioning: volume, directories, bronze table, schema seed, demo files."""
from __future__ import annotations

from framework.constants import BRONZE_DDL, BRONZE_TECHNICAL_COLUMNS
from framework.settings.feed_config import (
    load_active_config_row, apply_environment_policy,
    config_table_name as build_config_table_name,
    inventory_table_name as build_inventory_table_name,
    tgt_bronze_table as build_tgt_bronze_table,
    tgt_silver_table as build_tgt_silver_table,
    tgt_gold_table as build_tgt_gold_table,
    folder_paths, create_volume_sql,
    is_external_vendor_storage_uri,
    should_copy_demo_seed_files, copy_matching_seed_files,
    resolve_schema_seed_path,
)
from framework.provision.create_tables import (
    ensure_column_mapping_mode, ensure_bronze_lineage_columns,
    ensure_bronze_business_columns,
)
from framework.helpers.filename_parser import parse_filename_metadata


def load_cfg_and_paths(
    spark, catalog_name, bronze_schema_name, silver_schema_name, gold_schema_name,
    config_table_name_value, inventory_table_name_value, feed_key,
    runtime_settings=None,
):
    cfg = load_active_config_row(spark, catalog_name, bronze_schema_name, config_table_name_value, feed_key)
    cfg = apply_environment_policy(cfg, runtime_settings)
    return {
        "cfg": cfg,
        "config_table": build_config_table_name(catalog_name, bronze_schema_name, config_table_name_value),
        "inventory_table": build_inventory_table_name(catalog_name, bronze_schema_name, inventory_table_name_value),
        "bronze_table": build_tgt_bronze_table(catalog_name, bronze_schema_name, cfg),
        "silver_table": build_tgt_silver_table(catalog_name, silver_schema_name, cfg),
        "gold_table": build_tgt_gold_table(catalog_name, gold_schema_name, cfg),
        **folder_paths(catalog_name, bronze_schema_name, cfg),
    }


def bronze_business_columns(spark, table_name: str) -> list[str]:
    return [c for c in spark.table(table_name).columns if c not in BRONZE_TECHNICAL_COLUMNS]


def _apply_schema_read_policy(spark, seed_root: str, cfg: dict, bronze_table: str) -> None:
    from framework.helpers.schema_drift import load_schema_seed
    policy = str(cfg.get("schema_read_policy") or "FIRST_FILE").strip().upper()
    if policy == "FIRST_FILE":
        return
    schema_path = resolve_schema_seed_path(seed_root, cfg["tgt_bronze_table"])
    try:
        columns = load_schema_seed(schema_path, cfg["src_file_delimiter"])
    except FileNotFoundError:
        if policy == "SEED":
            raise
        return
    if columns:
        ensure_bronze_business_columns(spark, bronze_table, columns)



def _bronze_ddl_with_partitions(cfg: dict) -> str:
    """Build bronze DDL with optional PARTITIONED BY clause."""
    partition_cols_str = str(cfg.get("tgt_bronze_partition_cols") or "").strip()
    if not partition_cols_str:
        return BRONZE_DDL
    partition_cols = [c.strip() for c in partition_cols_str.split(",") if c.strip()]
    if not partition_cols:
        return BRONZE_DDL
    from framework.helpers.sql_helpers import quote_ident
    partition_clause = ", ".join(quote_ident(c) for c in partition_cols)
    return BRONZE_DDL.replace(
        ") USING DELTA",
        f") PARTITIONED BY ({partition_clause}) USING DELTA"
    )

def ensure_feed_environment(
    dbutils, spark, seed_root, catalog_name, bronze_schema_name,
    silver_schema_name, gold_schema_name, config_table_name,
    inventory_table_name, feed_key,
    *, copy_demo_seed_files=False, is_new_provision=True, runtime_settings=None,
) -> dict:
    resolved = load_cfg_and_paths(
        spark, catalog_name, bronze_schema_name, silver_schema_name, gold_schema_name,
        config_table_name, inventory_table_name, feed_key,
        runtime_settings=runtime_settings,
    )
    cfg = resolved["cfg"]
    spark.sql(create_volume_sql(catalog_name, bronze_schema_name, cfg))
    dirs = [resolved["request_dir"], resolved["temp_dir"], resolved["discovery_dir"], resolved["config_dir"]]
    if not is_external_vendor_storage_uri(resolved["source_dir"]):
        dirs.insert(0, resolved["source_dir"])
    for folder in dirs:
        dbutils.fs.mkdirs(folder)
    bronze_ddl = _bronze_ddl_with_partitions(cfg)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {resolved['bronze_table']} {bronze_ddl}")
    ensure_column_mapping_mode(spark, resolved["bronze_table"])
    ensure_bronze_lineage_columns(spark, resolved["bronze_table"])
    _apply_schema_read_policy(spark, seed_root, cfg, resolved["bronze_table"])
    should_copy = should_copy_demo_seed_files(cfg, copy_demo_seed_files, is_new_provision)
    copied = copy_matching_seed_files(seed_root, cfg, resolved["source_dir"]) if should_copy else []
    return {
        "feed_key": feed_key,
        "status": "OK",
        "bronze_table": resolved["bronze_table"],
        "source_dir": resolved["source_dir"],
        "seeded_source_file_names": copied,
    }
