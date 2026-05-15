from __future__ import annotations

import hashlib


def normalize_file_path_for_fingerprint(path: str) -> str:
    """Stable path form for fingerprint input (dbfs: URI, no trailing slash)."""
    p = (path or "").strip()
    if not p:
        return ""
    if p.startswith("dbfs:/"):
        return p.rstrip("/")
    if p.startswith("/Volumes/"):
        return f"dbfs:{p}".rstrip("/")
    return p.rstrip("/")


def compute_file_fingerprint(path: str, file_size: int | None, modification_time_ms: int | None) -> str:
    """
    SHA-256 hex over ``normalized_path|size|mtime_ms``.
    Uses literal ``none`` for missing size or mtime so fingerprints stay stable and explicit.
    """
    norm = normalize_file_path_for_fingerprint(path)
    sz = "" if file_size is None else str(int(file_size))
    mt = "" if modification_time_ms is None else str(int(modification_time_ms))
    raw = f"{norm}|{sz or 'none'}|{mt or 'none'}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def source_mtime_ms(entry: dict) -> int | None:
    v = entry.get("modificationTime")
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def source_size(entry: dict) -> int | None:
    v = entry.get("size")
    if v is None:
        v = entry.get("file_size")
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def enrich_source_entry(entry: dict) -> dict:
    """Add file_fingerprint, src_size, src_mtime_ms to a listing-shaped dict (mutates copy)."""
    out = dict(entry)
    path = str(out.get("file_path") or "")
    sz = source_size(out)
    mt = source_mtime_ms(out)
    out["file_size"] = sz
    out["src_size"] = sz
    out["src_mtime_ms"] = mt
    out["file_fingerprint"] = compute_file_fingerprint(path, sz, mt)
    return out
