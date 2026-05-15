import pytest

from framework.settings.environment import DEFAULT_FEED_KEY, resolve_runtime_settings


def test_resolve_dev():
    rt = resolve_runtime_settings("dev")
    assert rt["catalog_name"] == "hcb_dev"
    assert rt["bronze_schema_name"] == "ri_ops_ra_bronze"
    assert rt["config_table_name"] == "ops_cfg_file_ingestion"
    assert rt["_env"] == "dev"
    assert rt.get("copy_demo_seed_files") is False
    # default_feed_key intentionally removed — notebooks must fail fast on empty feed_key
    assert "default_feed_key" not in rt


def test_resolve_test():
    rt = resolve_runtime_settings("test")
    assert rt["catalog_name"] == "hcb_test"
    assert rt["bronze_schema_name"] == "ri_ops_ra_bronze"
    assert rt["_env"] == "test"


def test_resolve_empty_uses_default_env():
    rt = resolve_runtime_settings("")
    assert rt["_env"] == "dev"
    rt2 = resolve_runtime_settings("   ")
    assert rt2["_env"] == "dev"
    rt3 = resolve_runtime_settings(None)
    assert rt3["_env"] == "dev"


def test_resolve_unknown():
    with pytest.raises(ValueError, match="Unknown env"):
        resolve_runtime_settings("nope")


def test_resolve_prod_skips_demo_seeds():
    rt = resolve_runtime_settings("prod")
    assert rt["_env"] == "prod"
    assert rt["catalog_name"] == "hcb_prod"
    assert rt.get("copy_demo_seed_files") is False


def test_default_feed_key_is_defined():
    """DEFAULT_FEED_KEY exists for widget defaults; it must be a non-empty string."""
    assert isinstance(DEFAULT_FEED_KEY, str)
    assert len(DEFAULT_FEED_KEY.strip()) > 0
