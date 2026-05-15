"""Schema comparison and drift detection utilities."""
from __future__ import annotations


def parse_header_columns(file_text: str, delimiter: str, has_header: bool) -> list[str]:
    if not has_header or not file_text:
        return []
    first_line = file_text.splitlines()[0] if file_text.splitlines() else ""
    if not first_line.strip():
        return []
    return [x.strip() for x in first_line.split(delimiter)]


def compare_columns(source_columns: list[str], target_columns: list[str]) -> tuple[list[str], list[str]]:
    missing_in_file = [c for c in target_columns if c not in source_columns]
    new_in_file = [c for c in source_columns if c not in target_columns]
    return missing_in_file, new_in_file


def summarize_schema_change(missing_in_file: list[str], new_in_file: list[str], has_header: bool) -> tuple[str, str]:
    if not has_header:
        return "N", "header disabled; schema comparison skipped"
    if not missing_in_file and not new_in_file:
        return "N", "source columns match target schema"
    parts = []
    if missing_in_file:
        parts.append(f"missing_in_file={','.join(missing_in_file)}")
    if new_in_file:
        parts.append(f"new_in_file={','.join(new_in_file)}")
    return "Y", "; ".join(parts)


def load_schema_seed(schema_seed_path: str, delimiter: str) -> list[str]:
    from pathlib import Path
    p = Path(schema_seed_path)
    if not p.exists():
        raise FileNotFoundError(f"Schema seed file not found: {schema_seed_path}")
    text = p.read_text(encoding="utf-8-sig").strip()
    return parse_header_columns(text, delimiter, has_header=True)
