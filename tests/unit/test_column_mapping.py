from unittest.mock import MagicMock, call

from framework.provision.create_tables import ensure_column_mapping_mode
from framework.constants import BRONZE_DDL


def _mock_spark(table_exists=True, props=None):
    """Build a mock spark with configurable tableExists and SHOW TBLPROPERTIES result."""
    spark = MagicMock()
    spark.catalog.tableExists.return_value = table_exists
    if props is not None:
        rows = [MagicMock(__getitem__=lambda self, i, k=k, v=v: k if i == 0 else v) for k, v in props.items()]
        spark.sql.return_value.collect.return_value = rows
    return spark


def test_skips_when_table_does_not_exist():
    spark = _mock_spark(table_exists=False)
    ensure_column_mapping_mode(spark, "cat.schema.tbl")
    spark.sql.assert_not_called()


def test_no_alter_when_already_name_mode():
    spark = _mock_spark(props={"delta.columnMapping.mode": "name"})
    ensure_column_mapping_mode(spark, "cat.schema.tbl")
    # Only the SHOW TBLPROPERTIES call should have happened - no ALTER.
    spark.sql.assert_called_once()
    assert "SHOW TBLPROPERTIES" in spark.sql.call_args[0][0]


def test_alters_when_mode_is_none():
    spark = _mock_spark(props={"delta.columnMapping.mode": "none"})
    ensure_column_mapping_mode(spark, "cat.schema.tbl")
    assert spark.sql.call_count == 2
    alter_call = spark.sql.call_args_list[1][0][0]
    assert "ALTER TABLE" in alter_call
    assert "delta.columnMapping.mode" in alter_call
    assert "'name'" in alter_call


def test_alters_when_property_missing():
    spark = _mock_spark(props={"delta.someOtherProp": "true"})
    ensure_column_mapping_mode(spark, "cat.schema.bronze")
    assert spark.sql.call_count == 2
    alter_call = spark.sql.call_args_list[1][0][0]
    assert "ALTER TABLE cat.schema.bronze" in alter_call


def test_bronze_ddl_includes_column_mapping():
    assert "delta.columnMapping.mode" in BRONZE_DDL
    assert "'name'" in BRONZE_DDL
