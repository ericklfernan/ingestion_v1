from framework.helpers.zip_handler import dbfs_to_local_path, local_to_dbfs_path, extract_zip_text_files, cleanup_extract_dir
import zipfile


def test_dbfs_local_roundtrip():
    dbfs_path = "dbfs:/Volumes/a/b/c/file.zip"
    local_path = dbfs_to_local_path(dbfs_path)
    assert local_path == "/Volumes/a/b/c/file.zip"
    assert local_to_dbfs_path(local_path) == dbfs_path


def test_extract_zip_text_files(tmp_path):
    zip_path = tmp_path / "demo.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("inner/demo.txt", "a|b\n1|2\n")
        zf.writestr("inner/ignore.csv", "x,y\n")
    extract_dir = tmp_path / "extract"
    txt_paths = extract_zip_text_files(str(zip_path), str(extract_dir))
    assert len(txt_paths) == 1
    cleanup_extract_dir(str(extract_dir))
    assert not extract_dir.exists()
