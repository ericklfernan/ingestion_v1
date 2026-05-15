"""Spark StructType schemas for all tracking and inventory tables."""
from __future__ import annotations

try:
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, LongType,
        DoubleType, DateType, TimestampType, ArrayType,
    )
except ModuleNotFoundError:
    pass  # Schemas only used at Spark runtime


def job_log_schema():
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("task_name", StringType(), False),
        StructField("task_status", StringType(), False),
        StructField("feed_key", StringType(), False),
        StructField("status_reason", StringType(), True),
        StructField("dispatch_run_id", StringType(), True),
        StructField("ts_event", TimestampType(), False),
    ])


def file_log_schema():
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("file_path", StringType(), False),
        StructField("file_fingerprint", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("feed_key", StringType(), False),
        StructField("stage_name", StringType(), False),
        StructField("stage_status", StringType(), False),
        StructField("vendor_code", StringType(), True),
        StructField("lob_code", StringType(), True),
        StructField("file_date", DateType(), True),
        StructField("file_part_seq", IntegerType(), True),
        StructField("file_part_tot", IntegerType(), True),
        StructField("file_version_label", StringType(), True),
        StructField("file_version_rank", IntegerType(), True),
        StructField("file_extension", StringType(), True),
        StructField("status_reason", StringType(), True),
        StructField("cnt_row_written", LongType(), True),
        StructField("dispatch_run_id", StringType(), True),
        StructField("ts_event", TimestampType(), False),
    ])


def schema_change_schema():
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("file_path", StringType(), False),
        StructField("file_fingerprint", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("feed_key", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("source_columns_csv", StringType(), False),
        StructField("target_columns_csv", StringType(), False),
        StructField("missing_in_file", ArrayType(StringType()), False),
        StructField("new_in_file", ArrayType(StringType()), False),
        StructField("change_detected", StringType(), False),
        StructField("status_reason", StringType(), False),
        StructField("dispatch_run_id", StringType(), True),
        StructField("ts_event", TimestampType(), False),
    ])


def request_log_schema():
    return StructType([
        StructField("request_id", StringType(), False),
        StructField("feed_key", StringType(), False),
        StructField("dispatch_run_id", StringType(), True),
        StructField("request_type", StringType(), False),
        StructField("request_payload_json", StringType(), False),
        StructField("request_status_struct", StructType([
            StructField("status_code", StringType(), False),
            StructField("status_ts", TimestampType(), False),
        ]), False),
        StructField("arr_paths_requested", ArrayType(StringType()), False),
        StructField("arr_paths_pattern_valid", ArrayType(StringType()), False),
        StructField("arr_paths_pattern_invalid", ArrayType(StringType()), False),
        StructField("arr_paths_exist", ArrayType(StringType()), False),
        StructField("arr_paths_missing", ArrayType(StringType()), False),
        StructField("arr_paths_blocked_recent", ArrayType(StringType()), False),
        StructField("arr_paths_already_done", ArrayType(StringType()), False),
        StructField("arr_paths_ready", ArrayType(StringType()), False),
        StructField("arr_paths_in_inventory", ArrayType(StringType()), False),
        StructField("arr_paths_not_in_inventory", ArrayType(StringType()), False),
        StructField("arr_paths_final_eligible", ArrayType(StringType()), False),
        StructField("arr_paths_final_rejected", ArrayType(StringType()), False),
        StructField("arr_batch_inputs", ArrayType(StructType([
            StructField("batch_id", StringType(), False),
            StructField("batch_file_paths_json", StringType(), False),
            StructField("file_count", IntegerType(), False),
            StructField("total_size_bytes", LongType(), False),
        ])), False),
        StructField("ts_created", TimestampType(), False),
        StructField("ts_updated", TimestampType(), False),
    ])


def discovery_log_schema():
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("feed_key", StringType(), False),
        StructField("file_path", StringType(), False),
        StructField("file_fingerprint", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("request_payload_json", StringType(), False),
        StructField("status_reason", StringType(), False),
        StructField("dispatch_run_id", StringType(), True),
        StructField("ts_event", TimestampType(), False),
    ])


def notification_schema():
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("category", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("feed_key", StringType(), True),
        StructField("dispatch_run_id", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("message", StringType(), False),
        StructField("details_json", StringType(), True),
        StructField("resolved_recipients", StringType(), True),
        StructField("ts_event", TimestampType(), False),
    ])


def inventory_struct_schema():
    from framework.constants import INVENTORY_COLUMNS
    dtype_map = {
        "STRING": StringType(), "BIGINT": LongType(), "INT": IntegerType(),
        "DATE": DateType(), "TIMESTAMP": TimestampType(),
    }
    return StructType([StructField(name, dtype_map[dtype], True) for name, dtype in INVENTORY_COLUMNS])


def config_delta_struct_type():
    fields = []
    from framework.constants import CONFIG_COLUMNS
    dtype_map = {"STRING": StringType(), "INT": IntegerType(), "DOUBLE": DoubleType()}
    for name, dtype in CONFIG_COLUMNS:
        fields.append(StructField(name, dtype_map[dtype], True))
    fields.append(StructField("config_source_file", StringType(), True))
    fields.append(StructField("uc_source_dir", StringType(), True))
    fields.append(StructField("dispatch_run_id", StringType(), True))
    return StructType(fields)
