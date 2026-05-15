"""SQL helper utilities."""
from __future__ import annotations


def quote_ident(name: str) -> str:
    """Backtick-quote a column identifier, escaping embedded backticks."""
    return "`" + str(name).replace("`", "``") + "`"


def sql_string_literal(value: str) -> str:
    """Single-quote escape for SQL string literals."""
    return "'" + str(value).replace("'", "''") + "'"


def write_rows(spark, table_name: str, rows: list[dict], schema):
    """Append rows to a Delta table using the given Spark schema."""
    if rows:
        spark.createDataFrame(rows, schema=schema).write.format("delta").mode("append").saveAsTable(table_name)
