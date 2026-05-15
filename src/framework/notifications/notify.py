"""Notification record builder and recipient resolution."""
from __future__ import annotations

import json
from datetime import datetime, UTC
from uuid import uuid4

from .constants import VALID_SEVERITIES, VALID_CATEGORIES


def resolve_recipients(feed_recipients: str | None, env_override: str | None) -> str | None:
    override = str(env_override).strip() if env_override else None
    feed = str(feed_recipients).strip() if feed_recipients else None
    return override or feed or None


def notification_table_name(catalog_name: str, bronze_schema_name: str) -> str:
    return f"{catalog_name}.{bronze_schema_name}.ops_notifications"


def notification_create_sql(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_name}
(
  event_id STRING,
  severity STRING,
  category STRING,
  event_type STRING,
  feed_key STRING,
  dispatch_run_id STRING,
  request_id STRING,
  message STRING,
  details_json STRING,
  resolved_recipients STRING,
  ts_event TIMESTAMP
)
USING DELTA
""".strip()


def notification_missing_columns(existing_columns: list[str]) -> list[tuple[str, str]]:
    expected = [
        ("event_id", "STRING"), ("severity", "STRING"), ("category", "STRING"),
        ("event_type", "STRING"), ("feed_key", "STRING"), ("dispatch_run_id", "STRING"),
        ("request_id", "STRING"), ("message", "STRING"), ("details_json", "STRING"),
        ("resolved_recipients", "STRING"), ("ts_event", "TIMESTAMP"),
    ]
    existing = {c.lower() for c in existing_columns}
    return [(n, t) for n, t in expected if n.lower() not in existing]


def make_notification(
    severity: str,
    category: str,
    event_type: str,
    message: str,
    *,
    feed_key: str | None = None,
    dispatch_run_id: str | None = None,
    request_id: str | None = None,
    details: dict | None = None,
    resolved_recipients: str | None = None,
) -> dict:
    sev = str(severity).strip().upper()
    cat = str(category).strip().upper()
    if sev not in VALID_SEVERITIES:
        raise ValueError(f"Invalid severity={severity!r}; allowed: {sorted(VALID_SEVERITIES)}")
    if cat not in VALID_CATEGORIES:
        raise ValueError(f"Invalid category={category!r}; allowed: {sorted(VALID_CATEGORIES)}")
    return {
        "event_id": uuid4().hex,
        "severity": sev,
        "category": cat,
        "event_type": str(event_type).strip().upper(),
        "feed_key": str(feed_key) if feed_key else None,
        "dispatch_run_id": str(dispatch_run_id).strip() if dispatch_run_id else None,
        "request_id": str(request_id).strip() if request_id else None,
        "message": str(message),
        "details_json": json.dumps(details) if details else None,
        "resolved_recipients": str(resolved_recipients) if resolved_recipients else None,
        "ts_event": datetime.now(UTC),
    }
