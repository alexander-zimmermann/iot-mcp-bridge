from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from psycopg_pool import AsyncConnectionPool

from .config import Settings
from .logging import get_logger

log = get_logger(__name__)

_pool: AsyncConnectionPool | None = None


async def init_pool(settings: Settings) -> AsyncConnectionPool:
    global _pool
    if _pool is not None:
        return _pool
    _pool = AsyncConnectionPool(
        conninfo=settings.db_dsn,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
        kwargs={"autocommit": True, "row_factory": psycopg.rows.dict_row},
        open=False,
    )
    await _pool.open(wait=True, timeout=10.0)
    log.info(
        "db_pool_ready",
        host=settings.db_host,
        database=settings.db_name,
        user=settings.db_user,
        pool_min=settings.db_pool_min,
        pool_max=settings.db_pool_max,
    )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> AsyncConnectionPool:
    if _pool is None:
        raise RuntimeError("DB pool not initialised — call init_pool() first")
    return _pool


@asynccontextmanager
async def connection() -> AsyncIterator[psycopg.AsyncConnection]:
    async with get_pool().connection() as conn:
        yield conn


async def healthcheck() -> bool:
    try:
        async with connection() as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("db_healthcheck_failed", error=str(exc))
        return False
