from framework.helpers.fingerprint import enrich_source_entry
from framework.helpers.filename_parser import parse_filename_metadata
from pipelines.file_ingestion.manifest.build_manifest import discovery_rows


def _cfg():
    return {
        "feed_key": "retro_status_report_ci_aca",
        "feed_sub_key": "DEFAULT",
        "src_file_regex": r"^(CI)_Retro Status Report_(ACA)_(\d{8})_(\d{1,2})_(\d{1,2})(?:_(v\d+|updated))?\.(txt|zip)$",
        "src_file_capture_spec": "1|vendor_code|string;2|lob_code|string;3|file_date|date_yyyymmdd;4|file_part_seq|int;5|file_part_tot|int;6|file_version_label|string;7|file_extension|string",
    }


def test_parse_filename_metadata_updated_suffix_any_case():
    parsed = parse_filename_metadata("CI_Retro Status Report_ACA_20260102_1_5_UPDATED.txt", _cfg())
    assert parsed["parse_status"] == "PARSED"
    assert parsed["file_version_label"] == "UPDATED"
    assert parsed["file_version_rank"] == 1


def test_parse_filename_metadata():
    parsed = parse_filename_metadata("CI_Retro Status Report_ACA_20260102_1_5_v2.txt", _cfg())
    assert parsed["parse_status"] == "PARSED"
    assert parsed["vendor_code"] == "CI"
    assert parsed["lob_code"] == "ACA"
    assert parsed["file_part_seq"] == 1
    assert parsed["file_part_tot"] == 5
    assert parsed["file_extension"] == "txt"
    assert parsed["file_version_label"] == "v2"
    assert parsed["file_version_rank"] == 2
    assert parsed["delivery_group_key"] == "retro_status_report_ci_aca|CI|ACA|2026-01-02"
    assert parsed["part_group_key"] == "retro_status_report_ci_aca|CI|ACA|2026-01-02|1"


def test_discovery_rows_defaults():
    rows = discovery_rows(
        [
            enrich_source_entry(
                {
                    "file_name": "CI_Retro Status Report_ACA_20260102_1_5.txt",
                    "file_path": "dbfs:/Volumes/x/source/CI_Retro Status Report_ACA_20260102_1_5.txt",
                    "file_size": 123,
                }
            )
        ],
        _cfg(),
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["feed_key"] == "retro_status_report_ci_aca"
    assert row["file_name"] == "CI_Retro Status Report_ACA_20260102_1_5.txt"
    assert row["load_status"] == "DISCOVERED"
    assert row["parse_status"] == "PARSED"


# ---------------------------------------------------------------------------
# Tiered adjudication key tests (R4)
# ---------------------------------------------------------------------------

def _cfg_dated_only():
    """Config for a simple dated file: daily_claims_20260418.csv — no parts, no version."""
    return {
        "feed_key": "daily_claims",
        "feed_sub_key": "DEFAULT",
        "src_file_regex": r"^daily_claims_(\d{8})\.(csv)$",
        "src_file_capture_spec": "1|file_date|date_yyyymmdd;2|file_extension|string",
    }


def _cfg_bare():
    """Config for a bare file: vendor_export.txt — no date, no parts, no version."""
    return {
        "feed_key": "vendor_export",
        "feed_sub_key": "DEFAULT",
        "src_file_regex": r"^(vendor_export)\.(txt)$",
        "src_file_capture_spec": "1|vendor_code|string;2|file_extension|string",
    }


def test_tier_full_keys():
    """FULL tier: date + seq → part_group_key includes seq."""
    parsed = parse_filename_metadata("CI_Retro Status Report_ACA_20260102_1_5.txt", _cfg())
    assert parsed["delivery_group_key"] == "retro_status_report_ci_aca|CI|ACA|2026-01-02"
    assert parsed["part_group_key"] == "retro_status_report_ci_aca|CI|ACA|2026-01-02|1"
    assert parsed["delivery_group_key"] is not None
    assert parsed["part_group_key"] is not None


def test_tier_dated_keys():
    """DATED tier: date present, no seq → part_group_key = delivery_group_key."""
    parsed = parse_filename_metadata("daily_claims_20260418.csv", _cfg_dated_only())
    assert parsed["parse_status"] == "PARSED"
    assert parsed["file_date"].isoformat() == "2026-04-18"
    assert parsed["file_part_seq"] is None
    assert parsed["file_part_tot"] is None
    # DATED tier: both keys set, part_group_key equals delivery_group_key
    assert parsed["delivery_group_key"] == "daily_claims|||2026-04-18"
    assert parsed["part_group_key"] == "daily_claims|||2026-04-18"
    assert parsed["delivery_group_key"] is not None
    assert parsed["part_group_key"] is not None


def test_tier_bare_keys():
    """BARE tier: no date → delivery = feed_key, part = fk|file_name."""
    parsed = parse_filename_metadata("vendor_export.txt", _cfg_bare())
    assert parsed["parse_status"] == "PARSED"
    assert parsed["file_date"] is None
    assert parsed["file_part_seq"] is None
    # BARE tier: delivery is just the config key, part includes filename
    assert parsed["delivery_group_key"] == "vendor_export"
    assert parsed["part_group_key"] == "vendor_export|vendor_export.txt"
    assert parsed["delivery_group_key"] is not None
    assert parsed["part_group_key"] is not None


def test_parse_failed_keys_still_none():
    """PARSE_FAILED: keys remain None when regex doesn't match."""
    parsed = parse_filename_metadata("garbage_file.xyz", _cfg())
    assert parsed["parse_status"] == "PARSE_FAILED"
    assert parsed["delivery_group_key"] is None
    assert parsed["part_group_key"] is None


def test_tier_dated_with_version():
    """DATED tier with version label: date + version but no seq."""
    cfg = {
        "feed_key": "daily_report",
        "feed_sub_key": "DEFAULT",
        "src_file_regex": r"^daily_report_(\d{8})(?:_(v\d+))?\.(csv)$",
        "src_file_capture_spec": "1|file_date|date_yyyymmdd;2|file_version_label|string;3|file_extension|string",
    }
    parsed = parse_filename_metadata("daily_report_20260418_v3.csv", cfg)
    assert parsed["parse_status"] == "PARSED"
    assert parsed["file_version_label"] == "v3"
    assert parsed["file_version_rank"] == 3
    assert parsed["delivery_group_key"] == "daily_report|||2026-04-18"
    assert parsed["part_group_key"] == "daily_report|||2026-04-18"
