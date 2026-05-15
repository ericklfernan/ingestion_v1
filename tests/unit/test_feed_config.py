from framework.settings.feed_config import (
    resolve_schema_seed_path,
    normalize_config_row,
    folder_paths,
    format_uc_source_dir,
    resolve_vendor_source_dir,
    source_dir_request_prefix,
    is_external_vendor_storage_uri,
    should_copy_demo_seed_files,
    apply_environment_policy,
)
from framework.constants import CONFIG_COLUMNS


def test_normalize_config_row():
    cfg = normalize_config_row(
        {
            "feed_key": "retro_status_report_ci_aca",
            "feed_sub_key": "DEFAULT",
            "src_file_regex": r"^(CI)_Retro Status Report_(ACA)_(\d{8})_(\d{1,2})_(\d{1,2})(?:_(v\d+|updated))?\.(txt|zip)$",
            "src_file_capture_spec": "1|vendor_code|string;2|lob_code|string;3|file_date|date_yyyymmdd;4|file_part_seq|int;5|file_part_tot|int;6|file_version_label|string;7|file_extension|string",
            "tgt_bronze_table": "retro_status_report_ci_aca",
            "tgt_silver_table": "retro_status_report_ci_aca",
            "tgt_gold_table": "retro_status_report_ci_aca",
            "tgt_volume": "retro_status_report_ci_aca",
        }
    )
    folders = folder_paths("hcb_dev", "ri_ops_ra_bronze", cfg)
    assert cfg["batch_max_files"] == 10
    assert cfg["ctl_sync_config"] == "N"  # default when not provided
    assert folders["source_dir"].endswith("/retro_status_report_ci_aca/source")
    assert folders["source_dir"] == format_uc_source_dir(
        "hcb_dev", "ri_ops_ra_bronze", "retro_status_report_ci_aca", "source"
    )


def test_src_uri_overrides_uc_volume_path():
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "feed_sub_key": "DEFAULT",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
            "src_uri": "s3://my-lake/vendor/k/incoming",
        }
    )
    assert (
        resolve_vendor_source_dir("hcb_dev", "ri_ops_ra_bronze", cfg)
        == "s3://my-lake/vendor/k/incoming"
    )
    fp = folder_paths("hcb_dev", "ri_ops_ra_bronze", cfg)
    assert fp["source_dir"] == "s3://my-lake/vendor/k/incoming"
    assert source_dir_request_prefix(fp["source_dir"]) == "s3://my-lake/vendor/k/incoming"
    assert is_external_vendor_storage_uri(fp["source_dir"]) is True


def test_source_dir_request_prefix_uc_volume():
    assert source_dir_request_prefix("/Volumes/c/s/v/source") == "dbfs:/Volumes/c/s/v/source"


def test_ctl_demo_seed_policy_defaults_to_auto():
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
        }
    )
    assert cfg["ctl_demo_seed_policy"] == "AUTO"


def test_should_copy_demo_seed_files_respects_policy():
    assert should_copy_demo_seed_files({"ctl_demo_seed_policy": "COPY"}, default_enabled=False, is_new_provision=False) is True
    assert should_copy_demo_seed_files({"ctl_demo_seed_policy": "SKIP"}, default_enabled=True, is_new_provision=True) is False
    assert should_copy_demo_seed_files({"ctl_demo_seed_policy": "AUTO"}, default_enabled=True, is_new_provision=True) is True
    assert should_copy_demo_seed_files({"ctl_demo_seed_policy": "AUTO"}, default_enabled=True, is_new_provision=False) is False


def test_apply_environment_policy_requires_src_uri():
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
            "src_uri": "",
        }
    )
    try:
        apply_environment_policy(cfg, {"_env": "prod", "require_src_uri": True})
        assert False, "expected ValueError when src_uri is required"
    except ValueError as e:
        assert "requires src_uri" in str(e)


def test_apply_environment_policy_allows_src_uri_when_present():
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
            "src_uri": "s3://bucket/path",
        }
    )
    out = apply_environment_policy(cfg, {"_env": "prod", "require_src_uri": True})
    assert out["src_uri"] == "s3://bucket/path"


# --- schema_read_policy tests ---


def test_normalize_config_row_schema_read_policy_defaults_to_first_file():
    """No schema_read_policy provided -> defaults to FIRST_FILE."""
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
        }
    )
    assert cfg["schema_read_policy"] == "FIRST_FILE"


def test_normalize_config_row_schema_read_policy_seed():
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
            "schema_read_policy": "SEED",
        }
    )
    assert cfg["schema_read_policy"] == "SEED"


def test_normalize_config_row_schema_read_policy_auto():
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
            "schema_read_policy": "auto",
        }
    )
    assert cfg["schema_read_policy"] == "AUTO"


def test_normalize_config_row_schema_read_policy_invalid_falls_back():
    """Invalid policy value -> falls back to FIRST_FILE."""
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
            "schema_read_policy": "JUNK",
        }
    )
    assert cfg["schema_read_policy"] == "FIRST_FILE"


def test_normalize_config_row_schema_read_policy_none_value():
    """Explicit None -> defaults to FIRST_FILE."""
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
            "schema_read_policy": None,
        }
    )
    assert cfg["schema_read_policy"] == "FIRST_FILE"


def test_resolve_schema_seed_path():
    result = resolve_schema_seed_path("/repo/seeds", "retro_status_report_ci_aca")
    assert result.endswith("/seeds/schema/retro_status_report_ci_aca.txt")
    assert result.startswith("/repo/seeds/schema/")


# --- ctl_sync_config tests ---


def test_config_columns_contains_ctl_sync_config():
    """CONFIG_COLUMNS must include ctl_sync_config."""
    col_names = [name for name, _ in CONFIG_COLUMNS]
    assert "ctl_sync_config" in col_names


def test_normalize_config_row_ctl_sync_config_defaults_to_n():
    """No ctl_sync_config provided -> defaults to 'N'."""
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
        }
    )
    assert cfg["ctl_sync_config"] == "N"


def test_normalize_config_row_ctl_sync_config_explicit_y():
    """Explicit 'Y' is preserved."""
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
            "ctl_sync_config": "Y",
        }
    )
    assert cfg["ctl_sync_config"] == "Y"


def test_normalize_config_row_ctl_sync_config_lowercase_normalized():
    """Lowercase 'y' is normalized to uppercase 'Y'."""
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
            "ctl_sync_config": "y",
        }
    )
    assert cfg["ctl_sync_config"] == "Y"


def test_normalize_config_row_ctl_sync_config_invalid_falls_back():
    """Invalid value -> falls back to 'N'."""
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
            "ctl_sync_config": "MAYBE",
        }
    )
    assert cfg["ctl_sync_config"] == "N"


def test_normalize_config_row_ctl_sync_config_none_value():
    """Explicit None -> defaults to 'N'."""
    cfg = normalize_config_row(
        {
            "feed_key": "k",
            "src_file_regex": ".*",
            "src_file_capture_spec": "",
            "tgt_bronze_table": "b",
            "ctl_sync_config": None,
        }
    )
    assert cfg["ctl_sync_config"] == "N"
