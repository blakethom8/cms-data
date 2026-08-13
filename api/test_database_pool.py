import asyncio
import logging
import threading

import duckdb
import httpx
import pytest
from fastapi import FastAPI

from database_pool import (
    DatabasePoolMiddleware,
    DuckDBConnectionPool,
    request_connection,
)
from request_context import RequestContextMiddleware


class FakeConnection:
    def __init__(self, identity: int):
        self.identity = identity
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_pool_opens_fixed_independent_connections_and_closes_them() -> None:
    created: list[FakeConnection] = []

    def factory() -> FakeConnection:
        connection = FakeConnection(len(created))
        created.append(connection)
        return connection

    pool = DuckDBConnectionPool(factory, size=3, acquire_timeout_seconds=0.1)
    pool.start()

    assert [connection.identity for connection in created] == [0, 1, 2]

    pool.close()

    assert all(connection.closed for connection in created)


@pytest.mark.anyio
async def test_pool_owns_each_connection_exclusively_and_records_wait() -> None:
    connection = FakeConnection(1)
    pool = DuckDBConnectionPool(
        lambda: connection, size=1, acquire_timeout_seconds=0.2
    )
    pool.start()
    first = await pool.acquire()

    async def delayed_release() -> None:
        await asyncio.sleep(0.02)
        pool.release(first.connection)

    release_task = asyncio.create_task(delayed_release())
    second = await pool.acquire()
    await release_task

    assert second.connection is connection
    assert second.wait_ms >= 10
    pool.release(second.connection)
    pool.close()


@pytest.mark.anyio
async def test_middleware_moves_sync_database_work_off_loop_and_bounds_overload(
    caplog,
) -> None:
    pool = DuckDBConnectionPool(
        lambda: FakeConnection(7), size=1, acquire_timeout_seconds=0.02
    )
    pool.start()
    app = FastAPI()
    entered = threading.Event()
    release = threading.Event()

    @app.get("/profiles/{npi}")
    def profile(npi: str):
        connection = request_connection()
        entered.set()
        release.wait(timeout=1)
        return {"npi": npi, "connection": connection.identity}

    @app.get("/clinical-trials/version")
    async def version():
        return {"connection": request_connection()}

    app.add_middleware(
        DatabasePoolMiddleware,
        pool=pool,
        is_database_path=lambda path: path.startswith("/profiles"),
    )
    app.add_middleware(
        RequestContextMiddleware,
        resolve_key_name=lambda _key: "test",
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        with caplog.at_level(logging.INFO, logger="api.access"):
            first_task = asyncio.create_task(client.get("/profiles/1003005257"))
            await asyncio.to_thread(entered.wait, 1)
            unrelated = await client.get("/clinical-trials/version")
            overloaded = await client.get("/profiles/1003005258")
            release.set()
            first = await first_task

    assert first.status_code == 200
    assert first.json()["connection"] == 7
    assert "duckdb_pool;dur=" in first.headers["Server-Timing"]
    assert unrelated.status_code == 200
    assert unrelated.json() == {"connection": None}
    assert overloaded.status_code == 503
    assert overloaded.headers["Retry-After"] == "1"
    assert overloaded.headers["Server-Timing"] == "duckdb_pool;dur=20.00"
    assert overloaded.json() == {"detail": "CMS query capacity is temporarily busy"}
    assert "pool_result=acquired" in caplog.text
    assert "pool_result=overloaded" in caplog.text
    assert request_connection() is None
    pool.close()


@pytest.mark.anyio
async def test_unauthorized_database_request_does_not_consume_pool_capacity() -> None:
    pool = DuckDBConnectionPool(
        lambda: FakeConnection(9), size=1, acquire_timeout_seconds=0.01
    )
    pool.start()
    held = await pool.acquire()
    app = FastAPI()

    @app.get("/profiles/search")
    async def search():
        return {"connection": request_connection()}

    app.add_middleware(
        DatabasePoolMiddleware,
        pool=pool,
        is_database_path=lambda path: path.startswith("/profiles"),
        is_authorized=lambda request: request.headers.get("X-API-Key") == "valid",
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/profiles/search")

    assert response.status_code == 200
    assert response.json() == {"connection": None}
    assert "Server-Timing" not in response.headers
    pool.release(held.connection)
    pool.close()


@pytest.mark.anyio
async def test_real_duckdb_connections_execute_in_endpoint_worker_threads(
    tmp_path,
) -> None:
    database_path = tmp_path / "serving.duckdb"
    setup = duckdb.connect(str(database_path))
    setup.execute("create table providers (npi varchar, name varchar)")
    setup.execute("insert into providers values ('1003005257', 'Test Provider')")
    setup.close()

    pool = DuckDBConnectionPool(
        lambda: duckdb.connect(str(database_path), read_only=True),
        size=2,
        acquire_timeout_seconds=0.1,
    )
    pool.start()
    app = FastAPI()

    @app.get("/profiles/{npi}")
    def profile(npi: str):
        row = request_connection().execute(
            "select name from providers where npi = ?", [npi]
        ).fetchone()
        return {"name": row[0]}

    app.add_middleware(
        DatabasePoolMiddleware,
        pool=pool,
        is_database_path=lambda path: path.startswith("/profiles"),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        responses = await asyncio.gather(
            client.get("/profiles/1003005257"),
            client.get("/profiles/1003005257"),
        )

    assert [response.status_code for response in responses] == [200, 200]
    assert [response.json() for response in responses] == [
        {"name": "Test Provider"},
        {"name": "Test Provider"},
    ]
    pool.close()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
