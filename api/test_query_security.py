import duckdb
import pytest
from fastapi import HTTPException

import main


@pytest.fixture
def connection(monkeypatch):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE safe_table (value INTEGER, label VARCHAR)")
    conn.execute("INSERT INTO safe_table VALUES (1, 'one'), (2, 'two'), (3, 'three')")
    monkeypatch.setattr(main, "get_conn", lambda: conn)
    yield conn
    conn.close()


@pytest.mark.anyio
async def test_query_access_requires_an_allowlisted_named_consumer(monkeypatch) -> None:
    monkeypatch.setattr(
        main,
        "resolve_api_key_name",
        lambda key: {
            "operator-key": "command-center",
            "product-key": "ps-prod",
        }.get(key),
    )
    monkeypatch.setattr(main, "QUERY_CONSUMERS", frozenset({"command-center"}))

    assert await main.check_query_api_key("operator-key") is None
    with pytest.raises(HTTPException) as product_denied:
        await main.check_query_api_key("product-key")
    assert product_denied.value.status_code == 403
    with pytest.raises(HTTPException) as invalid_denied:
        await main.check_query_api_key("invalid")
    assert invalid_denied.value.status_code == 401


@pytest.mark.anyio
async def test_open_or_shared_access_is_not_operator_access(monkeypatch) -> None:
    monkeypatch.setattr(main, "QUERY_CONSUMERS", frozenset({"command-center"}))
    for consumer in ("open", "shared"):
        monkeypatch.setattr(main, "resolve_api_key_name", lambda _key, name=consumer: name)
        with pytest.raises(HTTPException) as denied:
            await main.check_query_api_key("presented")
        assert denied.value.status_code == 403


@pytest.mark.anyio
async def test_operator_query_has_an_unavoidable_outer_row_limit(connection) -> None:
    result = await main.run_query(main.QueryRequest(sql="SELECT * FROM safe_table", limit=2))

    assert result.row_count == 2
    assert result.truncated is True
    assert result.rows == [[1, "one"], [2, "two"]]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; SELECT 2",
        "ATTACH '/tmp/other.duckdb' AS other",
        "INSTALL httpfs",
        "LOAD httpfs",
        "PRAGMA version",
        "SELECT * FROM read_csv('/etc/passwd')",
        "SELECT * FROM read_parquet('https://example.test/data.parquet')",
        "SELECT * FROM glob('/tmp/*')",
        "SELECT * FROM range(1000000000000)",
        "SELECT repeat('x', 1000000000)",
        "CALL enable_logging()",
    ],
)
async def test_operator_query_rejects_multiple_control_and_external_io_statements(
    connection, sql: str
) -> None:
    with pytest.raises(HTTPException) as denied:
        await main.run_query(main.QueryRequest(sql=sql))

    assert denied.value.status_code == 403


@pytest.mark.anyio
async def test_table_schema_accepts_only_an_existing_main_table(connection) -> None:
    schema = await main.table_schema("safe_table")
    assert schema["table"] == "safe_table"
    assert [column["name"] for column in schema["columns"]] == ["value", "label"]

    with pytest.raises(HTTPException) as denied:
        await main.table_schema('safe_table; ATTACH "/tmp/other.duckdb" AS other')
    assert denied.value.status_code == 404

    attached = connection.execute(
        "SELECT count(*) FROM duckdb_databases() WHERE database_name = 'other'"
    ).fetchone()[0]
    assert attached == 0


@pytest.mark.anyio
async def test_operator_query_bounds_query_text_and_serialized_rows(
    connection, monkeypatch
) -> None:
    monkeypatch.setattr(main, "MAX_QUERY_SQL_CHARS", 10)
    with pytest.raises(HTTPException) as too_long:
        await main.run_query(main.QueryRequest(sql="SELECT value FROM safe_table"))
    assert too_long.value.status_code == 413

    monkeypatch.setattr(main, "MAX_QUERY_SQL_CHARS", 20_000)
    monkeypatch.setattr(main, "MAX_QUERY_RESPONSE_BYTES", 20)
    result = await main.run_query(main.QueryRequest(sql="SELECT * FROM safe_table"))
    assert result.truncated is True
    assert len(result.rows) < 3


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
