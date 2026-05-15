from framework.helpers.schema_drift import parse_header_columns, compare_columns, summarize_schema_change, load_schema_seed


def test_schema_compare():
    source = parse_header_columns("report_id|report_status|report_date\n1|OPEN|2026-01-02\n", "|", True)
    missing, new = compare_columns(source, ["report_id", "report_status", "report_date"])
    change, reason = summarize_schema_change(missing, new, True)
    assert change == "N"
    assert source == ["report_id", "report_status", "report_date"]


# --- load_schema_seed tests ---


def test_load_schema_seed_reads_pipe_delimited(tmp_path):
    """Happy path: reads a pipe-delimited schema seed file and returns column names."""
    seed_file = tmp_path / "test_feed.txt"
    seed_file.write_text("col_a|col_b|col_c\n", encoding="utf-8")
    columns = load_schema_seed(str(seed_file), "|")
    assert columns == ["col_a", "col_b", "col_c"]


def test_load_schema_seed_reads_comma_delimited(tmp_path):
    seed_file = tmp_path / "test_feed.txt"
    seed_file.write_text("first,second,third\n", encoding="utf-8")
    columns = load_schema_seed(str(seed_file), ",")
    assert columns == ["first", "second", "third"]


def test_load_schema_seed_file_not_found():
    """Missing seed file raises FileNotFoundError."""
    try:
        load_schema_seed("/nonexistent/path/feed.txt", "|")
        assert False, "expected FileNotFoundError"
    except FileNotFoundError as e:
        assert "Schema seed file not found" in str(e)


def test_load_schema_seed_empty_file(tmp_path):
    """Empty seed file returns empty list."""
    seed_file = tmp_path / "empty.txt"
    seed_file.write_text("", encoding="utf-8")
    columns = load_schema_seed(str(seed_file), "|")
    assert columns == []


def test_load_schema_seed_whitespace_only(tmp_path):
    """Whitespace-only seed file returns empty list."""
    seed_file = tmp_path / "blank.txt"
    seed_file.write_text("   \n  \n", encoding="utf-8")
    columns = load_schema_seed(str(seed_file), "|")
    assert columns == []


def test_load_schema_seed_many_columns(tmp_path):
    """Seed file with many columns (like retro_status_report) works correctly."""
    cols = [f"col_{i}" for i in range(130)]
    seed_file = tmp_path / "wide_feed.txt"
    seed_file.write_text("|".join(cols) + "\n", encoding="utf-8")
    result = load_schema_seed(str(seed_file), "|")
    assert len(result) == 130
    assert result[0] == "col_0"
    assert result[-1] == "col_129"
