"""Shared psycopg connection pools (dict rows, autocommit), opened in the app lifespan.

Two pools, not one: the read pool carries the SELECT-only role every query
tool uses, the write pool carries the role that may record an episode
verdict. Keeping them apart is what still makes the server read-only for
everything but that one table.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from .config import Settings
from .logging_setup import get_logger

log = get_logger(__name__)

# The pool is generic in the row type; we lock it to DictRow so cursor reads
# return dicts everywhere — both at runtime (via row_factory=dict_row in
# kwargs) and for static analysis (via the type parameter).
_pool: AsyncConnectionPool[psycopg.AsyncConnection[DictRow]] | None = None
_write_pool: AsyncConnectionPool[psycopg.AsyncConnection[DictRow]] | None = None

# The write pool serves one tool called by one person at a time; two
# connections is already generous.
_WRITE_POOL_MAX = 2


async def _open_pool(
    dsn: str, min_size: int, max_size: int
) -> AsyncConnectionPool[psycopg.AsyncConnection[DictRow]]:
    pool: AsyncConnectionPool[psycopg.AsyncConnection[DictRow]] = AsyncConnectionPool(
        conninfo=dsn,
        min_size=min_size,
        max_size=max_size,
        kwargs={"autocommit": True, "row_factory": dict_row},
        open=False,
    )
    await pool.open(wait=True, timeout=10.0)
    return pool


async def init_pool(
    settings: Settings,
) -> AsyncConnectionPool[psycopg.AsyncConnection[DictRow]]:
    """Open the module-level read pool. Idempotent — returns the existing pool if open."""
    global _pool
    if _pool is not None:
        return _pool
    _pool = await _open_pool(settings.db_dsn, settings.db_pool_min, settings.db_pool_max)
    log.info(
        "db_pool_ready",
        host=settings.db_host,
        database=settings.db_name,
        user=settings.db_username,
        pool_min=settings.db_pool_min,
        pool_max=settings.db_pool_max,
    )
    return _pool


async def init_write_pool(
    settings: Settings,
) -> AsyncConnectionPool[psycopg.AsyncConnection[DictRow]] | None:
    """Open the write pool when write credentials are configured.

    Without them the server runs read-only and this is a no-op — a missing
    credential must never keep the query tools from serving.
    """
    global _write_pool
    if _write_pool is not None:
        return _write_pool
    if not settings.db_write_enabled:
        log.info("db_write_pool_disabled", reason="no write credentials configured")
        return None
    _write_pool = await _open_pool(settings.db_write_dsn, 0, _WRITE_POOL_MAX)
    log.info(
        "db_write_pool_ready",
        host=settings.db_host,
        database=settings.db_name,
        user=settings.db_write_username,
    )
    return _write_pool


async def close_pool() -> None:
    """Close and drop both module-level pools (no-op when already closed)."""
    global _pool, _write_pool
    if _pool is not None:
        await _pool.close()
        _pool = None
    if _write_pool is not None:
        await _write_pool.close()
        _write_pool = None


def get_pool() -> AsyncConnectionPool[psycopg.AsyncConnection[DictRow]]:
    """Return the read pool, raising if ``init_pool()`` has not run yet."""
    if _pool is None:
        raise RuntimeError("DB pool not initialised — call init_pool() first")
    return _pool


def get_write_pool() -> AsyncConnectionPool[psycopg.AsyncConnection[DictRow]]:
    """Return the write pool, raising if the server was started read-only."""
    if _write_pool is None:
        raise RuntimeError(
            "no write credentials configured — set MCP_DB_WRITE_USERNAME /"
            " MCP_DB_WRITE_PASSWORD (or the *_FILE variants) to record verdicts"
        )
    return _write_pool


@asynccontextmanager
async def connection() -> AsyncIterator[psycopg.AsyncConnection[DictRow]]:
    """Borrow one pooled read connection for the duration of the ``async with`` block."""
    async with get_pool().connection() as conn:
        yield conn


@asynccontextmanager
async def write_connection() -> AsyncIterator[psycopg.AsyncConnection[DictRow]]:
    """Borrow one pooled write connection for the duration of the ``async with`` block."""
    async with get_write_pool().connection() as conn:
        yield conn


async def healthcheck() -> bool:
    """Round-trip ``SELECT 1``; False (never an exception) when the DB is unreachable."""
    try:
        async with connection() as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception as exc:
        log.warning("db_healthcheck_failed", error=str(exc))
        return False
