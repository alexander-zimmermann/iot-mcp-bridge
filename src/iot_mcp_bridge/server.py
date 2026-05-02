from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from . import auth as auth_module
from . import db
from .config import Settings, load_settings
from .logging import configure_logging, get_logger
from .tools import schema as schema_tools
from .tools import timeseries as timeseries_tools

log = get_logger(__name__)
mcp: FastMCP = FastMCP("iot-mcp-bridge")


@mcp.tool()
async def list_data_sources() -> list[dict[str, Any]]:
    """List hypertables and continuous aggregates with their time range.

    Use this first to discover what data is available before calling
    ``get_schema`` or ``query_timeseries``.
    """
    return await schema_tools.list_data_sources()


@mcp.tool()
async def get_schema(table: str) -> dict[str, Any]:
    """Describe a data source: columns, types, and JSONB key sample.

    For tables with a ``raw JSONB`` payload, returns the most common JSON keys
    observed in the latest 1000 rows so the LLM can construct
    ``raw->>'<key>'`` expressions.
    """
    return await schema_tools.get_schema(table)


@mcp.tool()
async def query_timeseries(
    table: str,
    columns: list[str],
    from_ts: str,
    to_ts: str,
    aggregation: str = "avg",
    bucket: str = "1 hour",
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Aggregated time-series query.

    - ``aggregation``: ``avg`` | ``sum`` | ``min`` | ``max`` | ``count``.
    - ``bucket``: Postgres interval literal, e.g. ``'15 minutes'``, ``'1 hour'``,
      ``'1 day'``.
    - When the bucket is at least one hour and a ``<table>_1h`` continuous
      aggregate exists, the query is routed to the aggregate automatically.
    - Result row count is capped (default 5000); exceed → error suggesting a
      coarser bucket.
    """
    return await timeseries_tools.query_timeseries(
        table=table,
        columns=columns,
        from_ts=from_ts,
        to_ts=to_ts,
        aggregation=aggregation,  # type: ignore[arg-type]
        bucket=bucket,
        filters=filters,
        settings=_settings,
    )


_settings: Settings | None = None


async def _healthz(_request: Request) -> JSONResponse:
    db_ok = await db.healthcheck()
    status = 200 if db_ok else 503
    return JSONResponse({"status": "ok" if db_ok else "degraded", "db": db_ok}, status_code=status)


async def _oauth_protected_resource(_request: Request) -> JSONResponse:
    if _settings is None:
        return JSONResponse({"error": "not_ready"}, status_code=503)
    return JSONResponse(auth_module.oauth_protected_resource_metadata(_settings))


def build_app() -> Starlette:
    global _settings
    _settings = load_settings()

    # FastMCP's StreamableHTTPSessionManager runs as part of mcp_app.lifespan;
    # the parent Starlette app must include it or tool calls fail with
    # "Task group is not initialized".
    mcp_app = mcp.http_app()

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        assert _settings is not None
        configure_logging(_settings.log_level, _settings.log_format)
        auth_module.configure(_settings)
        await db.init_pool(_settings)
        log.info(
            "iot_mcp_bridge_ready",
            host=_settings.host,
            port=_settings.port,
            auth_enabled=_settings.auth_enabled,
        )
        try:
            async with mcp_app.lifespan(app):
                yield
        finally:
            await db.close_pool()

    routes = [
        Route("/healthz", _healthz, methods=["GET"]),
        Route(
            "/.well-known/oauth-protected-resource",
            _oauth_protected_resource,
            methods=["GET"],
        ),
        Mount("/", app=mcp_app),
    ]
    middleware = [Middleware(auth_module.AuthMiddleware, settings=_settings)]
    return Starlette(routes=routes, middleware=middleware, lifespan=lifespan)


app = build_app()
