from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import structlog
from fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from . import auth as auth_module
from . import db
from . import metrics as metrics_module
from .config import Settings, load_settings
from .logging_setup import configure_logging, get_logger
from .tools import domain as domain_tools
from .tools import schema as schema_tools
from .tools import timeseries as timeseries_tools

log = get_logger(__name__)
mcp: FastMCP = FastMCP("iot-mcp-bridge")


def _principal_sub() -> str:
    """Return the OIDC ``sub`` bound by AuthMiddleware, or ``anonymous``."""
    sub = structlog.contextvars.get_contextvars().get("sub")
    return str(sub) if sub else "anonymous"


def _record_tool_call(tool: str, outcome: str) -> None:
    metrics_module.get().tool_calls.labels(tool=tool, sub=_principal_sub(), outcome=outcome).inc()


@mcp.tool()
async def list_data_sources() -> list[dict[str, Any]]:
    """List hypertables and continuous aggregates with their time range.

    Use this first to discover what data is available before calling
    ``get_schema`` or ``query_timeseries``.
    """
    log.info("tool_invoked", tool="list_data_sources")
    try:
        result = await schema_tools.list_data_sources()
    except Exception:
        _record_tool_call("list_data_sources", "error")
        raise
    _record_tool_call("list_data_sources", "ok")
    return result


@mcp.tool()
async def get_schema(table: str) -> dict[str, Any]:
    """Describe a data source: columns, types, and JSONB key sample.

    For tables with a ``raw JSONB`` payload, returns the most common JSON keys
    observed in the latest 1000 rows so the LLM can construct
    ``raw->>'<key>'`` expressions.
    """
    log.info("tool_invoked", tool="get_schema", table=table)
    try:
        result = await schema_tools.get_schema(table)
    except Exception:
        _record_tool_call("get_schema", "error")
        raise
    _record_tool_call("get_schema", "ok")
    return result


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
    log.info(
        "tool_invoked",
        tool="query_timeseries",
        table=table,
        bucket=bucket,
        aggregation=aggregation,
    )
    try:
        result = await timeseries_tools.query_timeseries(
            table=table,
            columns=columns,
            from_ts=from_ts,
            to_ts=to_ts,
            settings=_require_settings(),
            aggregation=aggregation,  # type: ignore[arg-type]
            bucket=bucket,
            filters=filters,
        )
    except Exception:
        _record_tool_call("query_timeseries", "error")
        raise
    _record_tool_call("query_timeseries", "ok")
    return result


@mcp.tool()
async def query_energy_flow(
    from_ts: str,
    to_ts: str,
    bucket: str = "1 hour",
) -> dict[str, Any]:
    """Joined PV / grid / consumer / battery / wallbox flow per bucket.

    Pulls from the hourly continuous aggregates ``solaredge_powerflow_1h`` and
    ``warp_meter_1h``. Bucket must be ``1 hour`` or coarser.
    """
    log.info("tool_invoked", tool="query_energy_flow", bucket=bucket)
    try:
        result = await domain_tools.query_energy_flow(
            from_ts=from_ts, to_ts=to_ts, settings=_require_settings(), bucket=bucket
        )
    except Exception:
        _record_tool_call("query_energy_flow", "error")
        raise
    _record_tool_call("query_energy_flow", "ok")
    return result


@mcp.tool()
async def query_heating_cycles(
    from_ts: str,
    to_ts: str,
    min_duration_seconds: int = 60,
) -> dict[str, Any]:
    """Detect ON/OFF burner cycles from boiler telemetry.

    A cycle is a contiguous run with ``curburnpow > 0`` over ``ems_esp``
    rows (``topic = 'boiler_data'``). Returns one row per cycle with start,
    end, duration, peak and average burner power.
    """
    log.info("tool_invoked", tool="query_heating_cycles")
    try:
        result = await domain_tools.query_heating_cycles(
            from_ts=from_ts,
            to_ts=to_ts,
            settings=_require_settings(),
            min_duration_seconds=min_duration_seconds,
        )
    except Exception:
        _record_tool_call("query_heating_cycles", "error")
        raise
    _record_tool_call("query_heating_cycles", "ok")
    return result


@mcp.tool()
async def query_room_climate(
    room: str,
    from_ts: str,
    to_ts: str,
    bucket: str = "1 hour",
    functions: list[str] | None = None,
) -> dict[str, Any]:
    """Bucketed average reading per GA name within a room.

    ``room`` is validated against the distinct rooms in ``knx_catalog`` so
    unknown rooms produce a precise error the LLM can self-correct against.
    Optional ``functions`` narrows the result to GAs whose ETS function name
    is in the given list.
    """
    log.info("tool_invoked", tool="query_room_climate", room=room, bucket=bucket)
    try:
        result = await domain_tools.query_room_climate(
            room=room,
            from_ts=from_ts,
            to_ts=to_ts,
            settings=_require_settings(),
            bucket=bucket,
            functions=functions,
        )
    except Exception:
        _record_tool_call("query_room_climate", "error")
        raise
    _record_tool_call("query_room_climate", "ok")
    return result


@mcp.tool()
async def query_knx_events(
    from_ts: str,
    to_ts: str,
    room: str | None = None,
    ga: str | None = None,
    name: str | None = None,
    functions: list[str] | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """Raw KNX event log against ``knx_catalog_view``.

    All predicates AND-combined; pass none for an unfiltered window.

    * ``room``      — exact match (e.g. ``"Wohnzimmer"``)
    * ``ga``        — exact GA match (``"4/2/161"``)
    * ``name``      — substring match against ``ga_name`` (``ILIKE``);
                      pass partial words like ``"Beleuchtung"``
    * ``functions`` — list of ETS function names; combine with ``room``
                      to narrow to e.g. all lighting events in a room

    Default ``limit`` 200; effective cap ``min(limit, query_row_limit)``.
    """
    log.info(
        "tool_invoked",
        tool="query_knx_events",
        room=room,
        ga=ga,
        name=name,
        functions=functions,
        limit=limit,
    )
    try:
        result = await domain_tools.query_knx_events(
            from_ts=from_ts,
            to_ts=to_ts,
            settings=_require_settings(),
            room=room,
            ga=ga,
            name=name,
            functions=functions,
            limit=limit,
        )
    except Exception:
        _record_tool_call("query_knx_events", "error")
        raise
    _record_tool_call("query_knx_events", "ok")
    return result


@mcp.tool()
async def query_unifi_events(
    from_ts: str,
    to_ts: str,
    camera: str | None = None,
    detection_type: str | None = None,
    event_type: str | None = None,
    min_score: int | None = None,
    event_id: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """Recent UniFi Protect Alarm Manager events for security review.

    All predicates AND-combined; pass none for an unfiltered window.

    * ``camera``         — exact match (``"fassade"``, ``"eingang"``,
                           ``"terrasse_wohnzimmer"``, ``"terrasse_esszimmer"``)
    * ``detection_type`` — trigger ``key`` (e.g. ``"person"``, ``"motion"``,
                           ``"line_crossed"``, ``"face_known"``, ``"vehicle"``,
                           ``"license_plate_known"``, ``"audio_alarm_siren"``)
    * ``event_type``     — UniFi source type (``"smartDetectZone"`` /
                           ``"smartDetectLine"`` / ``"motion"``)
    * ``min_score``      — confidence floor 0..100
    * ``event_id``       — exact UUID; use to fetch details for one alarm

    Default ``limit`` 200; effective cap ``min(limit, query_row_limit)``.
    """
    log.info(
        "tool_invoked",
        tool="query_unifi_events",
        camera=camera,
        detection_type=detection_type,
        event_type=event_type,
        min_score=min_score,
        event_id=event_id,
        limit=limit,
    )
    try:
        result = await domain_tools.query_unifi_events(
            from_ts=from_ts,
            to_ts=to_ts,
            settings=_require_settings(),
            camera=camera,
            detection_type=detection_type,
            event_type=event_type,
            min_score=min_score,
            event_id=event_id,
            limit=limit,
        )
    except Exception:
        _record_tool_call("query_unifi_events", "error")
        raise
    _record_tool_call("query_unifi_events", "ok")
    return result


@mcp.tool()
async def correlate_events(
    source_a: dict[str, Any],
    source_b: dict[str, Any],
    from_ts: str,
    to_ts: str,
    window: str = "15 minutes",
    bucket: str = "1 minute",
) -> dict[str, Any]:
    """Lagged Pearson correlation between two time-series streams.

    Each ``source`` is ``{"table": str, "column": str}``. Returns ``best`` and
    ``top`` (up to 10) lags by ``|corr|``.
    """
    log.info(
        "tool_invoked",
        tool="correlate_events",
        bucket=bucket,
        window=window,
    )
    try:
        result = await domain_tools.correlate_events(
            source_a=source_a,
            source_b=source_b,
            from_ts=from_ts,
            to_ts=to_ts,
            settings=_require_settings(),
            window=window,
            bucket=bucket,
        )
    except Exception:
        _record_tool_call("correlate_events", "error")
        raise
    _record_tool_call("correlate_events", "ok")
    return result


_settings: Settings | None = None


def _require_settings() -> Settings:
    """Return the module-level settings, raising if the app hasn't been built yet."""
    if _settings is None:
        raise RuntimeError("settings not initialised — build_app() must run first")
    return _settings


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
        metrics_module.init()
        auth_module.configure(_settings)
        await db.init_pool(_settings)
        metrics_server = await metrics_module.serve(metrics_module.get(), _settings.metrics_port)
        log.info(
            "iot_mcp_bridge_ready",
            host=_settings.host,
            port=_settings.port,
            metrics_port=_settings.metrics_port,
            auth_enabled=_settings.auth_enabled,
        )
        try:
            async with mcp_app.lifespan(app):
                yield
        finally:
            metrics_server.close()
            await metrics_server.wait_closed()
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
