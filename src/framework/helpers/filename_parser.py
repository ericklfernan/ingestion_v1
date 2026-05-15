"""Filename regex parsing and metadata extraction."""
from __future__ import annotations

import re
from datetime import datetime


def version_rank(version_label: str | None) -> int:
    if not version_label:
        return 0
    label = str(version_label).strip().lower()
    if label == "updated":
        return 1
    if label.startswith("v") and label[1:].isdigit():
        return int(label[1:])
    return 0


def capture_spec_list(cfg: dict) -> list[dict]:
    items = []
    for token in [x.strip() for x in str(cfg["src_file_capture_spec"]).split(";") if x.strip()]:
        group_index, target_column, data_type = token.split("|", 2)
        items.append({"group_index": int(group_index), "target_column": target_column, "data_type": data_type})
    return items


def parse_filename_metadata(file_name: str, cfg: dict) -> dict:
    m = re.match(cfg["src_file_regex"], file_name, flags=re.IGNORECASE)
    if not m:
        return {
            "file_name": file_name,
            "parse_status": "PARSE_FAILED",
            "parse_reason": "filename does not match configured regex",
            "vendor_code": None, "lob_code": None, "file_date": None,
            "file_part_seq": None, "file_part_tot": None,
            "file_version_label": None, "file_version_rank": 0,
            "file_extension": None, "delivery_group_key": None, "part_group_key": None,
        }
    result = {
        "file_name": file_name, "parse_status": "PARSED", "parse_reason": None,
        "vendor_code": None, "lob_code": None, "file_date": None,
        "file_part_seq": None, "file_part_tot": None,
        "file_version_label": None, "file_version_rank": 0, "file_extension": None,
    }
    for spec in capture_spec_list(cfg):
        raw = m.group(spec["group_index"])
        if spec["data_type"] == "int":
            value = int(raw) if raw is not None and str(raw).strip() else None
        elif spec["data_type"] == "date_yyyymmdd":
            value = datetime.strptime(raw, "%Y%m%d").date() if raw else None
        else:
            value = raw
        result[spec["target_column"]] = value
    result["file_version_rank"] = version_rank(result.get("file_version_label"))
    vendor = result.get("vendor_code") or ""
    lob = result.get("lob_code") or ""
    file_date = result.get("file_date")
    seq = result.get("file_part_seq")
    # --- Tiered adjudication keys (never NULL) ---
    # FULL:  date + seq present → part_group_key includes seq, delivery includes date
    # DATED: date present, no seq → part_group_key = delivery_group_key (no part distinction)
    # BARE:  no date → delivery = feed_key, part = feed_key|file_name
    fk = cfg["feed_key"]
    has_date = file_date is not None
    has_parts = has_date and seq is not None

    if has_parts:
        # Tier FULL: multi-part versioned files
        result["delivery_group_key"] = f"{fk}|{vendor}|{lob}|{file_date}"
        result["part_group_key"] = f"{fk}|{vendor}|{lob}|{file_date}|{seq}"
    elif has_date:
        # Tier DATED: single file per date, possible versions
        result["delivery_group_key"] = f"{fk}|{vendor}|{lob}|{file_date}"
        result["part_group_key"] = f"{fk}|{vendor}|{lob}|{file_date}"
    else:
        # Tier BARE: no date in filename — each file is its own group
        result["delivery_group_key"] = fk
        result["part_group_key"] = f"{fk}|{file_name}"

    return result
