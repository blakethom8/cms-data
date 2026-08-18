"""Bounded request-scoped ownership for synchronous DuckDB connections."""

from __future__ import annotations

import asyncio
import time
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Callable

import duckdb
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


_request_connections: ContextVar[dict[str, duckdb.DuckDBPyConnection]] = ContextVar(
    "duckdb_request_connections", default={}
)


class PoolAcquireTimeout(Exception):
    """Raised when bounded queue wait expires before a connection is available."""


@dataclass(frozen=True)
class ConnectionLease:
    connection: duckdb.DuckDBPyConnection
    wait_ms: float


class DuckDBConnectionPool:
    """Fixed-size pool; a connection has exactly one request owner at a time."""

    def __init__(
        self,
        connection_factory: Callable[[], duckdb.DuckDBPyConnection],
        *,
        size: int,
        acquire_timeout_seconds: float,
    ) -> None:
        if size < 1:
            raise ValueError("DuckDB pool size must be positive")
        if acquire_timeout_seconds <= 0:
            raise ValueError("DuckDB pool acquisition timeout must be positive")
        self._connection_factory = connection_factory
        self.size = size
        self.acquire_timeout_seconds = acquire_timeout_seconds
        self._available: asyncio.Queue[duckdb.DuckDBPyConnection] | None = None
        self._connections: list[duckdb.DuckDBPyConnection] = []

    def start(self) -> None:
        if self._available is not None:
            return
        available: asyncio.Queue[duckdb.DuckDBPyConnection] = asyncio.Queue(
            maxsize=self.size
        )
        try:
            for _ in range(self.size):
                connection = self._connection_factory()
                self._connections.append(connection)
                available.put_nowait(connection)
        except Exception:
            for connection in self._connections:
                connection.close()
            self._connections.clear()
            raise
        self._available = available

    async def acquire(self) -> ConnectionLease:
        if self._available is None:
            raise RuntimeError("DuckDB connection pool has not started")
        started = time.perf_counter()
        try:
            connection = await asyncio.wait_for(
                self._available.get(), timeout=self.acquire_timeout_seconds
            )
        except TimeoutError as exc:
            raise PoolAcquireTimeout from exc
        return ConnectionLease(
            connection=connection,
            wait_ms=(time.perf_counter() - started) * 1000,
        )

    def release(self, connection: duckdb.DuckDBPyConnection) -> None:
        if self._available is None:
            raise RuntimeError("DuckDB connection pool has not started")
        self._available.put_nowait(connection)

    def close(self) -> None:
        for connection in self._connections:
            connection.close()
        self._connections.clear()
        self._available = None


def request_connection(name: str = "warehouse") -> duckdb.DuckDBPyConnection | None:
    return _request_connections.get().get(name)


def bind_request_connection(
    connection: duckdb.DuckDBPyConnection, name: str = "warehouse"
) -> Token:
    connections = dict(_request_connections.get())
    connections[name] = connection
    return _request_connections.set(connections)


def reset_request_connection(token: Token) -> None:
    _request_connections.reset(token)


class DatabasePoolMiddleware(BaseHTTPMiddleware):
    """Lease one connection for database-only route execution."""

    def __init__(
        self,
        app,
        *,
        pool: DuckDBConnectionPool,
        is_database_path: Callable[[str], bool],
        is_authorized: Callable[[Request], bool] | None = None,
        connection_name: str = "warehouse",
    ) -> None:
        super().__init__(app)
        self._pool = pool
        self._is_database_path = is_database_path
        self._is_authorized = is_authorized
        self._connection_name = connection_name

    async def dispatch(self, request: Request, call_next) -> Response:
        if (
            request.method == "OPTIONS"
            or not self._is_database_path(request.url.path)
            or (self._is_authorized is not None and not self._is_authorized(request))
        ):
            return await call_next(request)

        try:
            lease = await self._pool.acquire()
        except PoolAcquireTimeout:
            request.state.pool_wait_ms = self._pool.acquire_timeout_seconds * 1000
            request.state.pool_result = "overloaded"
            return JSONResponse(
                status_code=503,
                content={"detail": "CMS query capacity is temporarily busy"},
                headers={
                    "Retry-After": "1",
                    "Server-Timing": (
                        "duckdb_pool;dur="
                        f"{self._pool.acquire_timeout_seconds * 1000:.2f}"
                    ),
                },
            )

        request.state.pool_wait_ms = lease.wait_ms
        request.state.pool_result = "acquired"
        token = bind_request_connection(lease.connection, self._connection_name)
        try:
            response = await call_next(request)
            response.headers["Server-Timing"] = f"duckdb_pool;dur={lease.wait_ms:.2f}"
            return response
        finally:
            reset_request_connection(token)
            self._pool.release(lease.connection)
