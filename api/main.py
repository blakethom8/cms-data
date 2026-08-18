"""
CMS Data API — lightweight DuckDB query service.
"""

import asyncio
import json
import os
import re
import sys
import threading
import time
from contextlib import asynccontextmanager
from typing import Optional

import duckdb
from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Production launches ``uvicorn api.main:app`` from the release root, while
# tests run from ``api/`` and import ``main`` directly. Route modules retain
# their historical top-level imports, so make the API directory available
# before importing the request-scoped database executor below.
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


def _resolve_utilization_db_path(database_path: str) -> str:
    configured = os.getenv("UTILIZATION_DUCKDB_PATH")
    if configured:
        return configured
    bundled = os.path.join(os.path.dirname(database_path), "utilization")
    return bundled if os.path.exists(bundled) else database_path


DB_PATH = os.getenv("DUCKDB_PATH", "/home/dataops/cms-data/data/provider_searcher.duckdb")
UTILIZATION_DB_PATH = _resolve_utilization_db_path(DB_PATH)
API_KEY = os.getenv("CMS_API_KEY", "")  # Set in production!
MAX_ROWS = int(os.getenv("MAX_ROWS", "1000"))
MAX_QUERY_SQL_CHARS = int(os.getenv("MAX_QUERY_SQL_CHARS", "20000"))
MAX_QUERY_RESPONSE_BYTES = int(os.getenv("MAX_QUERY_RESPONSE_BYTES", "1000000"))
MAX_QUERY_SECONDS = float(os.getenv("MAX_QUERY_SECONDS", "15"))
DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "2GB")
DUCKDB_THREADS = int(os.getenv("DUCKDB_THREADS", "4"))
DUCKDB_POOL_SIZE = int(os.getenv("DUCKDB_POOL_SIZE", "4"))
DUCKDB_POOL_ACQUIRE_SECONDS = float(os.getenv("DUCKDB_POOL_ACQUIRE_SECONDS", "2"))
UTILIZATION_DUCKDB_MEMORY_LIMIT = os.getenv(
    "UTILIZATION_DUCKDB_MEMORY_LIMIT", DUCKDB_MEMORY_LIMIT
)
UTILIZATION_DUCKDB_THREADS = int(
    os.getenv("UTILIZATION_DUCKDB_THREADS", str(DUCKDB_THREADS))
)
UTILIZATION_DUCKDB_POOL_SIZE = int(
    os.getenv("UTILIZATION_DUCKDB_POOL_SIZE", str(DUCKDB_POOL_SIZE))
)
UTILIZATION_DUCKDB_POOL_ACQUIRE_SECONDS = float(
    os.getenv(
        "UTILIZATION_DUCKDB_POOL_ACQUIRE_SECONDS", str(DUCKDB_POOL_ACQUIRE_SECONDS)
    )
)
READ_ONLY = True

# Fallback connection for startup checks and routes not yet moved into the pool.
_conn = None
_utilization_conn = None


def _connect_readonly() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(
        DB_PATH,
        read_only=READ_ONLY,
        config={
            "enable_external_access": "false",
            "allow_unsigned_extensions": "false",
            "memory_limit": DUCKDB_MEMORY_LIMIT,
            "threads": str(DUCKDB_THREADS),
        },
    )


def get_conn() -> duckdb.DuckDBPyConnection:
    from database_pool import request_connection

    scoped_connection = request_connection()
    if scoped_connection is not None:
        return scoped_connection
    global _conn
    if _conn is None:
        _conn = _connect_readonly()
    return _conn


def _connect_utilization_readonly() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(
        UTILIZATION_DB_PATH,
        read_only=READ_ONLY,
        config={
            "enable_external_access": "false",
            "allow_unsigned_extensions": "false",
            "memory_limit": UTILIZATION_DUCKDB_MEMORY_LIMIT,
            "threads": str(UTILIZATION_DUCKDB_THREADS),
        },
    )


def get_utilization_conn() -> duckdb.DuckDBPyConnection:
    from database_pool import request_connection

    scoped_connection = request_connection("utilization")
    if scoped_connection is not None:
        return scoped_connection
    global _utilization_conn
    if _utilization_conn is None:
        _utilization_conn = _connect_utilization_readonly()
    return _utilization_conn


from database_pool import DuckDBConnectionPool

database_pool = DuckDBConnectionPool(
    _connect_readonly,
    size=DUCKDB_POOL_SIZE,
    acquire_timeout_seconds=DUCKDB_POOL_ACQUIRE_SECONDS,
)

utilization_database_pool = DuckDBConnectionPool(
    _connect_utilization_readonly,
    size=UTILIZATION_DUCKDB_POOL_SIZE,
    acquire_timeout_seconds=UTILIZATION_DUCKDB_POOL_ACQUIRE_SECONDS,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: verify DB exists
    if not os.path.exists(DB_PATH):
        raise RuntimeError(f"Database not found: {DB_PATH}")
    if not os.path.exists(UTILIZATION_DB_PATH):
        raise RuntimeError(f"Utilization database not found: {UTILIZATION_DB_PATH}")
    get_conn()
    get_utilization_conn()
    try:
        database_pool.start()
        utilization_database_pool.start()
        yield
    finally:
        utilization_database_pool.close()
        database_pool.close()
        global _conn, _utilization_conn
        if _conn:
            _conn.close()
            _conn = None
        if _utilization_conn:
            _utilization_conn.close()
            _utilization_conn = None


app = FastAPI(
    title="CMS Provider Data API",
    version="0.2.0",
    lifespan=lifespan,
)

# --- Auth (defined before router includes so they can require it) ---
from auth import (
    API_KEY_HEADER,
    configured_consumer_names,
    make_key_resolver,
    parse_consumer_names,
)

SCOPED_API_KEYS = os.getenv("CMS_API_KEYS", "")
QUERY_CONSUMERS = parse_consumer_names(
    os.getenv("CMS_QUERY_CONSUMERS", "command-center")
)

api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)

# The one key resolver. Every enforcement point (route dependency, cache
# middleware, access log) goes through this, so a scoped key cannot be honored
# in one place and rejected in another.
resolve_api_key_name = make_key_resolver(API_KEY, SCOPED_API_KEYS)


async def check_api_key(key: Optional[str] = Security(api_key_header)):
    if resolve_api_key_name(key) is None:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


async def check_query_api_key(key: Optional[str] = Security(api_key_header)):
    """Require a named operator consumer for the legacy arbitrary-SQL route."""

    consumer = resolve_api_key_name(key)
    if consumer is None:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    if consumer not in QUERY_CONSUMERS:
        raise HTTPException(
            status_code=403,
            detail="Arbitrary SQL access is restricted to approved operator consumers",
        )


# Data routers — every route requires the X-API-Key header.
_secured = [Depends(check_api_key)]

from match import get_match_router
app.include_router(get_match_router(get_conn), dependencies=_secured)

from places_match import get_search_router
app.include_router(get_search_router(get_conn), dependencies=_secured)

from unified_search import get_unified_router
app.include_router(get_unified_router(get_conn), dependencies=_secured)

from practices import get_practices_router
app.include_router(get_practices_router(get_conn), dependencies=_secured)

from market_snapshot import get_market_snapshot_router
app.include_router(get_market_snapshot_router(get_conn), dependencies=_secured)

from explorer import get_explorer_router
app.include_router(get_explorer_router(get_conn), dependencies=_secured)

from profiles import get_profiles_router
app.include_router(get_profiles_router(get_conn), dependencies=_secured)

from industry import get_industry_router
app.include_router(get_industry_router(get_conn), dependencies=_secured)

from utilization import get_utilization_router
app.include_router(get_utilization_router(get_utilization_conn), dependencies=_secured)

from research import get_research_router
app.include_router(get_research_router(get_conn), dependencies=_secured)

from clinical_trials import get_clinical_trials_router
app.include_router(get_clinical_trials_router(), dependencies=_secured)

from radar import get_radar_router
app.include_router(get_radar_router(get_conn), dependencies=_secured)

from operations import get_operations_router
app.include_router(get_operations_router(get_conn), dependencies=_secured)

from release_info import ReleaseCacheMiddleware, get_release_router, make_release_resolver
release_resolver = make_release_resolver(DB_PATH)
app.include_router(get_release_router(release_resolver), dependencies=_secured)

from database_pool import DatabasePoolMiddleware

_DATABASE_ROUTE_PREFIXES = (
    "/profiles", "/practices", "/radar", "/explorer"
)
_DATABASE_ROUTE_EXCLUSIONS = {"/profiles/exemplars", "/explorer/showcases"}


def _is_pooled_database_path(path: str) -> bool:
    return path not in _DATABASE_ROUTE_EXCLUSIONS and any(
        path == prefix or path.startswith(prefix + "/")
        for prefix in _DATABASE_ROUTE_PREFIXES
    )


# Added before ReleaseCacheMiddleware, so an ETag hit can return 304 without
# leasing a database connection. RequestContextMiddleware remains outermost.
app.add_middleware(
    DatabasePoolMiddleware,
    pool=database_pool,
    is_database_path=_is_pooled_database_path,
    is_authorized=lambda request: resolve_api_key_name(
        request.headers.get(API_KEY_HEADER)
    )
    is not None,
)

app.add_middleware(
    DatabasePoolMiddleware,
    pool=utilization_database_pool,
    is_database_path=lambda path: path == "/utilization"
    or path.startswith("/utilization/"),
    is_authorized=lambda request: resolve_api_key_name(
        request.headers.get(API_KEY_HEADER)
    )
    is not None,
    connection_name="utilization",
)

app.add_middleware(
    ReleaseCacheMiddleware,
    resolve_metadata=release_resolver,
    is_authorized=lambda request: resolve_api_key_name(request.headers.get(API_KEY_HEADER))
    is not None,
)

# Added after ReleaseCacheMiddleware, so it wraps it: conditional requests that
# short-circuit to 304 still get their correlation ID echoed and logged.
from request_context import RequestContextMiddleware
app.add_middleware(
    RequestContextMiddleware,
    resolve_key_name=resolve_api_key_name,
    resolve_release=release_resolver,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten in production
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# --- Models ---

class QueryRequest(BaseModel):
    sql: str
    limit: Optional[int] = None  # Override default max rows

class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[list]
    row_count: int
    truncated: bool
    elapsed_ms: float


# --- Routes ---

@app.get("/health")
async def health():
    conn = get_conn()
    row = conn.execute("SELECT count(*) FROM core_providers").fetchone()
    return {"status": "ok", "core_providers": row[0]}


_FORBIDDEN_QUERY_TOKENS = re.compile(
    r"\b(?:attach|detach|install|load|copy|export|import|pragma|call|set|reset|"
    r"create|alter|drop|insert|update|delete|merge|truncate|vacuum|checkpoint|"
    r"read_csv(?:_auto)?|read_json(?:_auto)?|read_ndjson(?:_auto)?|read_parquet|"
    r"parquet_scan|csv_scan|json_scan|glob|sqlite_scan|postgres_scan|mysql_scan|"
    r"iceberg_scan|delta_scan|duckdb_secrets|which_secret|range|generate_series|"
    r"repeat)\b",
    re.IGNORECASE,
)


def _validated_operator_query(conn, sql: str) -> str:
    """Accept exactly one SELECT and reject external-I/O or control features."""

    if len(sql) > MAX_QUERY_SQL_CHARS:
        raise HTTPException(status_code=413, detail="Query text exceeds the configured limit")
    try:
        statements = conn.extract_statements(sql)
    except duckdb.Error as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
        raise HTTPException(status_code=403, detail="Only one SELECT statement is allowed")
    if _FORBIDDEN_QUERY_TOKENS.search(sql):
        raise HTTPException(
            status_code=403,
            detail="Query uses a feature unavailable through the operator SQL endpoint",
        )
    return statements[0].query.strip().rstrip(";")


class _OperatorQueryTimedOut(Exception):
    """Internal marker used to avoid exposing DuckDB interruption details."""


def _execute_operator_query(sql: str, limit: int) -> tuple[list[str], list[tuple]]:
    """Run operator SQL on an interruptible cursor outside the event loop."""

    query_conn = get_conn().cursor()
    timed_out = threading.Event()

    def interrupt_query() -> None:
        timed_out.set()
        query_conn.interrupt()

    timer = threading.Timer(MAX_QUERY_SECONDS, interrupt_query)
    timer.daemon = True
    timer.start()
    try:
        result = query_conn.execute(
            f"SELECT * FROM ({sql}) AS operator_query LIMIT ?", [limit + 1]
        )
        columns = [desc[0] for desc in result.description]
        return columns, result.fetchmany(limit + 1)
    except duckdb.Error as exc:
        if timed_out.is_set():
            raise _OperatorQueryTimedOut from exc
        raise
    finally:
        timer.cancel()
        timer.join()
        query_conn.close()


@app.post(
    "/query",
    response_model=QueryResponse,
    dependencies=[Depends(check_query_api_key)],
)
async def run_query(req: QueryRequest):
    sql = req.sql.strip().rstrip(";")
    limit = max(1, min(req.limit or MAX_ROWS, MAX_ROWS))
    conn = get_conn()
    sql = _validated_operator_query(conn, sql)
    t0 = time.perf_counter()
    try:
        # An outer bound cannot be bypassed by a LIMIT token in a comment,
        # string literal, CTE, or nested subquery.
        columns, fetched_rows = await asyncio.to_thread(
            _execute_operator_query, sql, limit
        )
    except _OperatorQueryTimedOut as exc:
        raise HTTPException(
            status_code=408, detail="Query exceeded the configured execution time limit"
        ) from exc
    except duckdb.OutOfMemoryException as exc:
        raise HTTPException(
            status_code=422, detail="Query exceeded the configured memory limit"
        ) from exc
    except duckdb.Error as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    elapsed = (time.perf_counter() - t0) * 1000

    truncated = len(fetched_rows) > limit
    rows: list[list] = []
    response_bytes = len(json.dumps(columns, separators=(",", ":")).encode("utf-8"))
    for raw_row in fetched_rows[:limit]:
        row = list(raw_row)
        row_bytes = len(
            json.dumps(row, default=str, separators=(",", ":")).encode("utf-8")
        )
        if response_bytes + row_bytes > MAX_QUERY_RESPONSE_BYTES:
            truncated = True
            break
        rows.append(row)
        response_bytes += row_bytes

    return QueryResponse(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        truncated=truncated,
        elapsed_ms=round(elapsed, 2),
    )


@app.get("/tables", dependencies=[Depends(check_api_key)])
async def list_tables():
    conn = get_conn()
    result = conn.execute("""
        SELECT table_name, 
               estimated_size as approx_rows
        FROM duckdb_tables()
        WHERE schema_name = 'main'
        ORDER BY table_name
    """)
    tables = [{"name": r[0], "approx_rows": r[1]} for r in result.fetchall()]
    return {"tables": tables}


@app.get("/tables/{table_name}/schema", dependencies=[Depends(check_api_key)])
async def table_schema(table_name: str):
    conn = get_conn()
    exists = conn.execute(
        """
        SELECT 1
        FROM duckdb_tables()
        WHERE schema_name = 'main' AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    if exists is None:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    quoted_table = '"' + table_name.replace('"', '""') + '"'
    try:
        result = conn.execute(f"DESCRIBE {quoted_table}")
        cols = [{"name": r[0], "type": r[1], "nullable": r[2]} for r in result.fetchall()]
    except duckdb.Error:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    return {"table": table_name, "columns": cols}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
