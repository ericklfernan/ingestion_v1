"""Create and evolve ops tables (job_log, file_log, schema_change_log, request_log, discovery_log, notifications, inventory)."""
from __future__ import annotations

from framework.constants import BRONZE_DDL, BRONZE_LINEAGE_COLUMNS, INVENTORY_COLUMNS
from framework.tracking.table_names import core_tables
from framework.tracking.ddl import (
    job_log_create_sql, file_log_create_sql, schema_change_log_create_sql,
    request_log_create_sql, discovery_log_create_sql, dispatch_state_create_sql,
    alter_add_columns_sql, missing_dispatch_run_id_only,
    file_log_missing_columns, schema_change_log_missing_columns,
    discovery_log_missing_columns, request_log_missing_columns,
)
from framework.notifications.notify import notification_create_sql, notification_missing_columns
from framework.settings.feed_config import (
    config_table_name as build_config_table_name,
    inventory_table_name as build_inventory_table_name,
)
from framework.helpers.sql_helpers import quote_ident


# ---------------------------------------------------------------------------
# Inventory DDL
# ---------------------------------------------------------------------------

def inventory_create_sql(table_name: str) -> str:
    cols = ",\n  ".join([f"{name} {dtype}" for name, dtype in INVENTORY_COLUMNS])
    return f"CREATE TABLE IF NOT EXISTS {table_name}\n(\n  {cols}\n)\nUSING DELTA"


def inventory_missing_columns(existing_columns: list[str]) -> list[tuple[str, str]]:
    existing = {c.lower() for c in existing_columns}
    return [(name, dtype) for name, dtype in INVENTORY_COLUMNS if name.lower() not in existing]


def inventory_alter_add_columns_sql(table_name: str, missing_columns: list[tuple[str, str]]) -> str:
    cols = ", ".join([f"{n} {t}" for n, t in missing_columns])
    return f"ALTER TABLE {table_name} ADD COLUMNS ({cols})"


# ---------------------------------------------------------------------------
# Schema evolution helpers
# ---------------------------------------------------------------------------

def ensure_dispatch_run_id_column(spark, table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        return
    missing = missing_dispatch_run_id_only(spark.table(table_name).columns)
    if missing:
        spark.sql(alter_add_columns_sql(table_name, missing))


def ensure_bronze_lineage_columns(spark, table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        return
    existing_lower = {c.lower() for c in spark.table(table_name).columns}
    missing = [(name, dtype) for name, dtype in BRONZE_LINEAGE_COLUMNS if name.lower() not in existing_lower]
    if not missing:
        return
    add_cols_sql = ", ".join([f"{name} {dtype}" for name, dtype in missing])
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({add_cols_sql})")


def ensure_column_mapping_mode(spark, table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        return
    props = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
    current = {r[0]: r[1] for r in props}
    if current.get("delta.columnMapping.mode") != "name":
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")


def ensure_bronze_business_columns(spark, table_name: str, source_columns: list[str]) -> None:
    if not source_columns:
        return
    existing_lower = {c.lower() for c in spark.table(table_name).columns}
    missing = [c for c in source_columns if c and str(c).strip() and str(c).lower() not in existing_lower]
    if not missing:
        return
    add_cols_sql = ", ".join([f"{quote_ident(c)} STRING" for c in missing])
    try:
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({add_cols_sql})")
    except Exception as _col_exc:
        msg = str(_col_exc)
        if "FIELD_ALREADY_EXISTS" in msg or "DELTA_DUPLICATE_COLUMNS_FOUND" in msg or "DELTA_METADATA_CHANGED" in msg:
            pass  # concurrent batch already added these columns — goal met
        else:
            raise


# ---------------------------------------------------------------------------
# Ensure all ops tables exist with correct schema
# ---------------------------------------------------------------------------

def ensure_ops_tables(spark, catalog_name: str, bronze_schema_name: str, inventory_table_name_value: str) -> dict:
    inv = build_inventory_table_name(catalog_name, bronze_schema_name, inventory_table_name_value)
    if not spark.catalog.tableExists(inv):
        spark.sql(inventory_create_sql(inv))
    else:
        missing = inventory_missing_columns(spark.table(inv).columns)
        if missing:
            spark.sql(inventory_alter_add_columns_sql(inv, missing))

    tables = core_tables(catalog_name, bronze_schema_name)

    spark.sql(job_log_create_sql(tables["job_log"]))
    ensure_dispatch_run_id_column(spark, tables["job_log"])

    spark.sql(file_log_create_sql(tables["file_log"]))
    ensure_dispatch_run_id_column(spark, tables["file_log"])

    spark.sql(schema_change_log_create_sql(tables["schema_change_log"]))
    ensure_dispatch_run_id_column(spark, tables["schema_change_log"])

    spark.sql(request_log_create_sql(tables["request_log"]))
    if spark.catalog.tableExists(tables["request_log"]):
        missing_req = request_log_missing_columns(spark.table(tables["request_log"]).columns)
        if missing_req:
            spark.sql(alter_add_columns_sql(tables["request_log"], missing_req))
    ensure_dispatch_run_id_column(spark, tables["request_log"])

    spark.sql(discovery_log_create_sql(tables["discovery_log"]))
    ensure_dispatch_run_id_column(spark, tables["discovery_log"])

    for tname, misser in (
        (tables["file_log"], file_log_missing_columns),
        (tables["schema_change_log"], schema_change_log_missing_columns),
        (tables["discovery_log"], discovery_log_missing_columns),
    ):
        if spark.catalog.tableExists(tname):
            miss = misser(spark.table(tname).columns)
            if miss:
                spark.sql(alter_add_columns_sql(tname, miss))

    spark.sql(notification_create_sql(tables["notifications"]))
    if spark.catalog.tableExists(tables["notifications"]):
        notif_miss = notification_missing_columns(spark.table(tables["notifications"]).columns)
        if notif_miss:
            spark.sql(alter_add_columns_sql(tables["notifications"], notif_miss))

    spark.sql(dispatch_state_create_sql(tables["dispatch_state"]))

    return tables
