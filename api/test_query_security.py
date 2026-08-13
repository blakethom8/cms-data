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


@pytest.mark.anyio
async def test_operator_query_interrupts_runaway_recursive_work(
    connection, monkeypatch
) -> None:
    monkeypatch.setattr(main, "MAX_QUERY_SECONDS", 0.01)

    with pytest.raises(HTTPException) as timed_out:
        await main.run_query(
            main.QueryRequest(
                sql="""
                    WITH RECURSIVE work(value) AS (
                        SELECT 1
                        UNION ALL
                        SELECT value + 1 FROM work WHERE value < 1000000000
                    )
                    SELECT sum(value) FROM work
                """
            )
        )

    assert timed_out.value.status_code == 408
    assert "execution time limit" in timed_out.value.detail


@pytest.mark.anyio
async def test_operator_query_maps_memory_exhaustion_without_leaking_details(
    connection, monkeypatch
) -> None:
    def exceed_memory(_sql: str, _limit: int):
        raise duckdb.OutOfMemoryException("host-specific allocator details")

    monkeypatch.setattr(main, "_execute_operator_query", exceed_memory)

    with pytest.raises(HTTPException) as exhausted:
        await main.run_query(main.QueryRequest(sql="SELECT * FROM safe_table"))

    assert exhausted.value.status_code == 422
    assert exhausted.value.detail == "Query exceeded the configured memory limit"


def test_serving_connection_configures_process_resource_bounds(monkeypatch) -> None:
    captured: dict = {}
    connection = duckdb.connect(":memory:")

    def connect(path, *, read_only, config):
        captured.update(path=path, read_only=read_only, config=config)
        return connection

    monkeypatch.setattr(main, "_conn", None)
    monkeypatch.setattr(main.duckdb, "connect", connect)

    assert main.get_conn() is connection
    assert captured["config"]["memory_limit"] == main.DUCKDB_MEMORY_LIMIT
    assert captured["config"]["threads"] == str(main.DUCKDB_THREADS)
    assert captured["config"]["enable_external_access"] == "false"

    monkeypatch.setattr(main, "_conn", None)
    connection.close()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
