from __future__ import annotations

import shutil
import uuid
import zipfile
from pathlib import Path


def dbfs_to_local_path(path: str) -> str:
    value = path.strip()
    if value.startswith("dbfs:/Volumes/"):
        return "/Volumes/" + value[len("dbfs:/Volumes/"):]
    if value.startswith("/Volumes/"):
        return value
    if value.startswith("dbfs:/"):
        return "/dbfs/" + value[len("dbfs:/"):].lstrip("/")
    return value


def local_to_dbfs_path(path: str) -> str:
    value = path.strip()
    if value.startswith("/Volumes/"):
        return "dbfs:" + value
    if value.startswith("/dbfs/"):
        return "dbfs:/" + value[len("/dbfs/"):].lstrip("/")
    return value


def build_extract_dirs(catalog_name: str, bronze_schema_name: str, tgt_volume: str, dir_temp: str, zip_file_name: str) -> tuple[str, str]:
    stem = Path(zip_file_name).stem
    token = uuid.uuid4().hex[:8]
    dbfs_dir = f"dbfs:/Volumes/{catalog_name}/{bronze_schema_name}/{tgt_volume}/{dir_temp}/{stem}_{token}"
    local_dir = dbfs_to_local_path(dbfs_dir)
    return dbfs_dir, local_dir


def extract_zip_text_files(zip_dbfs_path: str, extract_local_dir: str) -> list[str]:
    zip_local_path = dbfs_to_local_path(zip_dbfs_path)
    extract_dir = Path(extract_local_dir)
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)

    text_local_paths = []
    with zipfile.ZipFile(zip_local_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            zf.extract(info, extract_dir)
            candidate = extract_dir / info.filename
            if candidate.suffix.lower() == ".txt":
                text_local_paths.append(str(candidate))

    text_local_paths = sorted(text_local_paths)
    if not text_local_paths:
        raise ValueError(f"No .txt files found inside zip: {zip_dbfs_path}")
    return text_local_paths


def cleanup_extract_dir(extract_local_dir: str) -> None:
    p = Path(extract_local_dir)
    if p.exists():
        shutil.rmtree(p)
