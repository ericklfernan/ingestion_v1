from datetime import UTC, datetime, timedelta

from pipelines.file_ingestion.orchestrate.evaluate_schedule import is_dispatch_due, should_auto_trigger_row
from pipelines.file_ingestion.orchestrate.scan_config import csv_row_to_delta_dict, collect_config_rows_from_disk, deduplicate_config_rows


# --- is_dispatch_due tests (Quartz 6-field format) ---


def test_is_dispatch_due_empty_cron():
    assert is_dispatch_due("", None) is False


def test_is_dispatch_due_first_run():
    """Never dispatched → always due."""
    assert is_dispatch_due("0 0 * * * ?", None) is True


def test_is_dispatch_due_hourly_within_window():
    """Hourly cron, last dispatched 30 min ago → not yet due (next fire is ~30 min away)."""
    now = datetime(2026, 5, 4, 14, 30, 0, tzinfo=UTC)
    last = datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC)  # Last at 14:00, next fire = 15:00
    assert is_dispatch_due("0 0 * * * ?", last, now_utc=now) is False


def test_is_dispatch_due_hourly_after_fire():
    """Hourly cron, last dispatched 2 hours ago → due (next fire long passed)."""
    now = datetime(2026, 5, 4, 16, 1, 0, tzinfo=UTC)
    last = datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC)  # Last at 14:00, next fire = 15:00
    assert is_dispatch_due("0 0 * * * ?", last, now_utc=now) is True


def test_is_dispatch_due_daily_within_24h():
    """Daily at midnight cron, last dispatched 12h ago → not yet due."""
    now = datetime(2026, 5, 4, 12, 0, 0, tzinfo=UTC)
    last = datetime(2026, 5, 4, 0, 0, 0, tzinfo=UTC)  # Last at midnight, next fire = tomorrow midnight
    assert is_dispatch_due("0 0 0 * * ?", last, now_utc=now) is False


def test_is_dispatch_due_daily_after_24h():
    """Daily at midnight cron, last dispatched 25h ago → due."""
    now = datetime(2026, 5, 5, 1, 0, 0, tzinfo=UTC)
    last = datetime(2026, 5, 4, 0, 0, 0, tzinfo=UTC)  # Last at May 4 midnight, next fire = May 5 midnight
    assert is_dispatch_due("0 0 0 * * ?", last, now_utc=now) is True


def test_is_dispatch_due_every_5_minutes_due():
    """Every 5 min cron, last dispatched 6 min ago → due."""
    now = datetime(2026, 5, 4, 14, 11, 0, tzinfo=UTC)
    last = datetime(2026, 5, 4, 14, 5, 0, tzinfo=UTC)  # Last at :05, next fire = :10
    assert is_dispatch_due("0 */5 * * * ?", last, now_utc=now) is True


def test_is_dispatch_due_every_5_minutes_not_due():
    """Every 5 min cron, last dispatched 3 min ago → not yet due."""
    now = datetime(2026, 5, 4, 14, 8, 0, tzinfo=UTC)
    last = datetime(2026, 5, 4, 14, 5, 0, tzinfo=UTC)  # Last at :05, next fire = :10
    assert is_dispatch_due("0 */5 * * * ?", last, now_utc=now) is False


def test_is_dispatch_due_fixed_minute_hourly():
    """Fires at :23 every hour. Last at :23, now at :50 → not yet due (next fire = next hour :23)."""
    now = datetime(2026, 5, 4, 14, 50, 0, tzinfo=UTC)
    last = datetime(2026, 5, 4, 14, 23, 0, tzinfo=UTC)  # Last at 14:23, next fire = 15:23
    assert is_dispatch_due("0 23 * * * ?", last, now_utc=now) is False


def test_is_dispatch_due_invalid_5field_returns_false():
    """Old 5-field format is no longer supported → returns False (not None/error)."""
    assert is_dispatch_due("*/5 * * * *", None) is True  # None last → always due regardless
    now = datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC)
    last = datetime(2026, 5, 4, 13, 0, 0, tzinfo=UTC)
    # 5-field not parseable → _next_fire_time returns None → is_dispatch_due returns False
    assert is_dispatch_due("*/5 * * * *", last, now_utc=now) is False


# --- should_auto_trigger_row tests (Quartz 6-field format) ---


def test_should_auto_trigger_row_respects_auto_flag():
    now = datetime(2026, 5, 4, 15, 0, 0, tzinfo=UTC)
    row = {"ctl_active": "Y", "ctl_auto_trigger": "N", "sched_cron": "0 0 * * * ?", "last_dispatched_at": None}
    assert should_auto_trigger_row(row, now_utc=now) is False


def test_should_auto_trigger_row_respects_maintenance_hold():
    now = datetime(2026, 5, 4, 15, 0, 0, tzinfo=UTC)
    row = {
        "ctl_active": "Y",
        "ctl_auto_trigger": "Y",
        "sched_cron": "0 0 * * * ?",
        "last_dispatched_at": None,
        "ctl_maintenance_hold_until": (now + timedelta(hours=1)).isoformat(),
    }
    assert should_auto_trigger_row(row, now_utc=now) is False


def test_should_auto_trigger_row_happy_path():
    now = datetime(2026, 5, 4, 15, 1, 0, tzinfo=UTC)
    row = {
        "ctl_active": "Y",
        "ctl_auto_trigger": "Y",
        "sched_cron": "0 0 * * * ?",
        "last_dispatched_at": datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC),
        "ctl_maintenance_hold_until": "",
    }
    assert should_auto_trigger_row(row, now_utc=now) is True


def test_should_auto_trigger_row_can_ignore_config_toggle_by_policy():
    now = datetime(2026, 5, 4, 15, 1, 0, tzinfo=UTC)
    row = {
        "ctl_active": "Y",
        "ctl_auto_trigger": "N",
        "sched_cron": "0 0 * * * ?",
        "last_dispatched_at": datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC),
    }
    assert should_auto_trigger_row(row, now_utc=now, honor_config_auto_trigger=False) is True


def test_should_auto_trigger_row_can_ignore_hold_by_policy():
    now = datetime(2026, 5, 4, 15, 1, 0, tzinfo=UTC)
    row = {
        "ctl_active": "Y",
        "ctl_auto_trigger": "Y",
        "sched_cron": "0 0 * * * ?",
        "last_dispatched_at": datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC),
        "ctl_maintenance_hold_until": (now + timedelta(hours=3)).isoformat(),
    }
    assert should_auto_trigger_row(row, now_utc=now, honor_maintenance_hold=False) is True


# --- schema_read_policy default test ---


def test_csv_row_to_delta_dict_defaults_schema_read_policy():
    """CSV row with no schema_read_policy defaults to FIRST_FILE."""
    raw = {
        "feed_key": "test_feed",
        "src_file_regex": ".*",
        "src_file_capture_spec": "",
        "tgt_bronze_table": "test_table",
    }
    result = csv_row_to_delta_dict(raw, "test_config.csv")
    assert result["schema_read_policy"] == "FIRST_FILE"


def test_csv_row_to_delta_dict_preserves_schema_read_policy():
    """CSV row with explicit schema_read_policy preserves it."""
    raw = {
        "feed_key": "test_feed",
        "src_file_regex": ".*",
        "src_file_capture_spec": "",
        "tgt_bronze_table": "test_table",
        "schema_read_policy": "SEED",
    }
    result = csv_row_to_delta_dict(raw, "test_config.csv")
    assert result["schema_read_policy"] == "SEED"


# --- ctl_sync_config tests ---


def test_csv_row_to_delta_dict_defaults_ctl_sync_config():
    """CSV row with no ctl_sync_config defaults to 'N'."""
    raw = {
        "feed_key": "test_feed",
        "src_file_regex": ".*",
        "src_file_capture_spec": "",
        "tgt_bronze_table": "test_table",
    }
    result = csv_row_to_delta_dict(raw, "test_config.csv")
    assert result["ctl_sync_config"] == "N"


def test_csv_row_to_delta_dict_preserves_ctl_sync_config_y():
    """CSV row with explicit ctl_sync_config='Y' preserves it."""
    raw = {
        "feed_key": "test_feed",
        "src_file_regex": ".*",
        "src_file_capture_spec": "",
        "tgt_bronze_table": "test_table",
        "ctl_sync_config": "Y",
    }
    result = csv_row_to_delta_dict(raw, "test_config.csv")
    assert result["ctl_sync_config"] == "Y"


def test_csv_row_to_delta_dict_empty_ctl_sync_config_defaults():
    """CSV row with empty string ctl_sync_config falls back to default 'N'."""
    raw = {
        "feed_key": "test_feed",
        "src_file_regex": ".*",
        "src_file_capture_spec": "",
        "tgt_bronze_table": "test_table",
        "ctl_sync_config": "",
    }
    result = csv_row_to_delta_dict(raw, "test_config.csv")
    assert result["ctl_sync_config"] == "N"

def test_csv_row_to_delta_dict_no_last_dispatched_at():
    """last_dispatched_at belongs in ops_dispatch_state, not in config rows."""
    raw = {
        "feed_key": "test_feed",
        "src_file_regex": ".*",
        "src_file_capture_spec": "",
        "tgt_bronze_table": "test_table",
    }
    result = csv_row_to_delta_dict(raw, "test_config.csv")
    assert "last_dispatched_at" not in result




def test_collect_config_rows_from_disk_reads_ctl_sync_config(tmp_path):
    """CSV with ctl_sync_config column is parsed correctly."""
    csv_file = tmp_path / "config.csv"
    csv_file.write_text(
        "feed_key,src_file_regex,src_file_capture_spec,tgt_bronze_table,ctl_sync_config\n"
        "feed_a,.*,,feed_a,Y\n"
        "feed_b,.*,,feed_b,N\n"
    )
    rows = collect_config_rows_from_disk(str(tmp_path))
    assert len(rows) == 2
    row_a = [r for r in rows if r["feed_key"] == "feed_a"][0]
    row_b = [r for r in rows if r["feed_key"] == "feed_b"][0]
    assert row_a["ctl_sync_config"] == "Y"
    assert row_b["ctl_sync_config"] == "N"


# --- empty config tests ---


def test_collect_config_rows_from_disk_empty_dir(tmp_path):
    """Empty config directory returns [] instead of raising."""
    rows = collect_config_rows_from_disk(str(tmp_path))
    assert rows == []


def test_collect_config_rows_from_disk_csv_with_no_data_rows(tmp_path):
    """CSV with only a header and no data rows returns []."""
    csv_file = tmp_path / "empty_config.csv"
    csv_file.write_text("feed_key,src_file_regex,tgt_bronze_table\n")
    rows = collect_config_rows_from_disk(str(tmp_path))
    assert rows == []


def test_collect_config_rows_from_disk_missing_dir():
    """Missing directory raises FileNotFoundError."""
    try:
        collect_config_rows_from_disk("/nonexistent/config/dir")
        assert False, "expected FileNotFoundError"
    except FileNotFoundError:
        pass


# --- deduplicate_config_rows tests ---


def _make_row(fk, sub_key="DEFAULT", source_file="config_a.csv", mtime=0.0):
    """Helper to build a minimal config row dict for dedup tests."""
    return {
        "feed_key": fk,
        "feed_sub_key": sub_key,
        "config_source_file": source_file,
        "config_source_mtime": mtime,
        "src_file_regex": ".*",
        "tgt_bronze_table": fk,
    }


def test_deduplicate_no_duplicates():
    """No duplicates -> all rows kept, empty dropped log."""
    rows = [_make_row("feed_a", source_file="a.csv"), _make_row("feed_b", source_file="b.csv")]
    winners, dropped = deduplicate_config_rows(rows)
    assert len(winners) == 2
    assert len(dropped) == 0


def test_deduplicate_cross_file_latest_wins():
    """Same key in two files -> file with latest modified time wins."""
    rows = [
        _make_row("feed_x", source_file="config_01.csv", mtime=100.0),
        _make_row("feed_x", source_file="config_02.csv", mtime=200.0),
    ]
    winners, dropped = deduplicate_config_rows(rows)
    assert len(winners) == 1
    assert winners[0]["config_source_file"] == "config_02.csv"
    assert len(dropped) == 1
    assert dropped[0]["winner_from"] == "config_02.csv"
    assert dropped[0]["dropped_from"] == "config_01.csv"


def test_deduplicate_cross_file_mtime_beats_filename_order():
    """Newer file (by mtime) wins even if its filename sorts earlier alphabetically."""
    rows = [
        _make_row("feed_z", source_file="zzz_old.csv", mtime=100.0),
        _make_row("feed_z", source_file="aaa_new.csv", mtime=200.0),
    ]
    winners, dropped = deduplicate_config_rows(rows)
    assert len(winners) == 1
    assert winners[0]["config_source_file"] == "aaa_new.csv"
    assert dropped[0]["winner_from"] == "aaa_new.csv"
    assert dropped[0]["dropped_from"] == "zzz_old.csv"



def test_deduplicate_same_file_earliest_row_wins():
    """Same key twice in the same file -> earliest row (first encountered) wins."""
    rows = [
        _make_row("feed_y", source_file="config.csv", mtime=100.0),
        {**_make_row("feed_y", source_file="config.csv", mtime=100.0), "tgt_bronze_table": "feed_y_v2"},
    ]
    winners, dropped = deduplicate_config_rows(rows)
    assert len(winners) == 1
    assert winners[0]["tgt_bronze_table"] == "feed_y"  # first row kept
    assert len(dropped) == 1
    assert "same file" in dropped[0]["reason"]


def test_deduplicate_mixed_scenario():
    """Mix of unique, cross-file dupe, and same-file dupe."""
    rows = [
        _make_row("unique_feed", source_file="a.csv", mtime=100.0),
        _make_row("cross_file_dupe", source_file="a.csv", mtime=100.0),
        _make_row("cross_file_dupe", source_file="b.csv", mtime=200.0),  # b.csv newer, wins over a.csv
        _make_row("same_file_dupe", source_file="c.csv", mtime=150.0),
        {**_make_row("same_file_dupe", source_file="c.csv", mtime=150.0), "tgt_bronze_table": "v2"},  # first row wins
    ]
    winners, dropped = deduplicate_config_rows(rows)
    winner_keys = {w["feed_key"] for w in winners}
    assert winner_keys == {"unique_feed", "cross_file_dupe", "same_file_dupe"}
    assert len(winners) == 3
    assert len(dropped) == 2

    # cross_file_dupe: b.csv wins
    cross_winner = [w for w in winners if w["feed_key"] == "cross_file_dupe"][0]
    assert cross_winner["config_source_file"] == "b.csv"

    # same_file_dupe: first row wins (tgt_bronze_table == "same_file_dupe", not "v2")
    same_winner = [w for w in winners if w["feed_key"] == "same_file_dupe"][0]
    assert same_winner["tgt_bronze_table"] == "same_file_dupe"


def test_deduplicate_non_duplicates_unaffected_by_duplicates():
    """Non-duplicate feeds must never be dropped even when duplicates exist."""
    rows = [
        _make_row("good_feed_1", source_file="a.csv", mtime=100.0),
        _make_row("dupe_feed", source_file="a.csv", mtime=100.0),
        _make_row("good_feed_2", source_file="b.csv", mtime=200.0),
        _make_row("dupe_feed", source_file="b.csv", mtime=200.0),
        _make_row("good_feed_3", source_file="b.csv", mtime=200.0),
    ]
    winners, dropped = deduplicate_config_rows(rows)
    winner_keys = {w["feed_key"] for w in winners}
    assert "good_feed_1" in winner_keys
    assert "good_feed_2" in winner_keys
    assert "good_feed_3" in winner_keys
    assert "dupe_feed" in winner_keys
    assert len(winners) == 4
    assert len(dropped) == 1
