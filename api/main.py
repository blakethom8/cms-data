"""
CMS Data API — lightweight DuckDB query service.
"""

import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import duckdb
from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

DB_PATH = os.getenv("DUCKDB_PATH", "/home/dataops/cms-data/data/provider_searcher.duckdb")
API_KEY = os.getenv("CMS_API_KEY", "")  # Set in production!
MAX_ROWS = int(os.getenv("MAX_ROWS", "1000"))
READ_ONLY = True

# Connection pool (DuckDB is single-writer but supports multiple read cursors)
_conn = None

def get_conn() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        _conn = duckdb.connect(DB_PATH, read_only=READ_ONLY)
    return _conn


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: verify DB exists
    if not os.path.exists(DB_PATH):
        raise RuntimeError(f"Database not found: {DB_PATH}")
    get_conn()
    yield
    # Shutdown
    global _conn
    if _conn:
        _conn.close()
        _conn = None


app = FastAPI(
    title="CMS Provider Data API",
    version="0.2.0",
    lifespan=lifespan,
)

# Match engine
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from match import get_match_router
match_router = get_match_router(get_conn)
app.include_router(match_router)

from places_match import get_search_router
search_router = get_search_router(get_conn)
app.include_router(search_router)

from unified_search import get_unified_router
unified_router = get_unified_router(get_conn)
app.include_router(unified_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten in production
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# --- Auth ---

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def check_api_key(key: Optional[str] = Security(api_key_header)):
    if not API_KEY:
        return  # No key configured = open access (dev mode)
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


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


@app.post("/query", response_model=QueryResponse, dependencies=[Depends(check_api_key)])
async def run_query(req: QueryRequest):
    sql = req.sql.strip().rstrip(";")

    # Block writes
    first_word = sql.split()[0].upper() if sql else ""
    if first_word in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "COPY"):
        raise HTTPException(status_code=403, detail="Write operations not allowed")

    limit = min(req.limit or MAX_ROWS, MAX_ROWS)

    # Wrap in a limit if not already present
    sql_upper = sql.upper()
    if "LIMIT" not in sql_upper:
        sql = f"{sql} LIMIT {limit + 1}"

    conn = get_conn()
    t0 = time.perf_counter()
    try:
        result = conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchmany(limit + 1)
    except duckdb.Error as e:
        raise HTTPException(status_code=400, detail=str(e))
    elapsed = (time.perf_counter() - t0) * 1000

    truncated = len(rows) > limit
    if truncated:
        rows = rows[:limit]

    # Convert to plain lists (DuckDB returns tuples)
    rows = [list(r) for r in rows]

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
    try:
        result = conn.execute(f"DESCRIBE {table_name}")
        cols = [{"name": r[0], "type": r[1], "nullable": r[2]} for r in result.fetchall()]
    except duckdb.Error:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    return {"table": table_name, "columns": cols}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
