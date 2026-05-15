"""Centralized log table name builders."""
from __future__ import annotations


def job_log_table_name(catalog_name: str, bronze_schema_name: str) -> str:
    return f"{catalog_name}.{bronze_schema_name}.ops_job_log"


def file_log_table_name(catalog_name: str, bronze_schema_name: str) -> str:
    return f"{catalog_name}.{bronze_schema_name}.ops_file_log"


def schema_change_log_table_name(catalog_name: str, bronze_schema_name: str) -> str:
    return f"{catalog_name}.{bronze_schema_name}.ops_file_schema_change_log"


def request_log_table_name(catalog_name: str, bronze_schema_name: str) -> str:
    return f"{catalog_name}.{bronze_schema_name}.ops_request_log"


def discovery_log_table_name(catalog_name: str, bronze_schema_name: str) -> str:
    return f"{catalog_name}.{bronze_schema_name}.ops_discovery_log"


def dispatch_state_table_name(catalog_name: str, bronze_schema_name: str) -> str:
    return f"{catalog_name}.{bronze_schema_name}.ops_dispatch_state"


def core_tables(catalog_name: str, bronze_schema_name: str) -> dict:
    """Build all core table names in one call."""
    from framework.notifications.notify import notification_table_name
    return {
        "job_log": job_log_table_name(catalog_name, bronze_schema_name),
        "file_log": file_log_table_name(catalog_name, bronze_schema_name),
        "schema_change_log": schema_change_log_table_name(catalog_name, bronze_schema_name),
        "request_log": request_log_table_name(catalog_name, bronze_schema_name),
        "discovery_log": discovery_log_table_name(catalog_name, bronze_schema_name),
        "notifications": notification_table_name(catalog_name, bronze_schema_name),
        "dispatch_state": dispatch_state_table_name(catalog_name, bronze_schema_name),
    }
