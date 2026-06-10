"""MCP server wiring: tool registration, HTTP app factory, and lifespan.

Docstring convention: the ``@mcp.tool()`` docstrings in this module ARE the
tool descriptions served to the LLM over MCP — they are the API contract and
must stay accurate and self-contained. The docstrings on the implementations
in ``tools/*`` document internals for developers and must not be relied on
by clients.

The app is exposed via :func:`build_app` (uvicorn factory) so importing this
module has no side effects — settings are only loaded when the app is built.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable
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
from . import nats as nats_module
from .config import Settings, load_settings
from .logging_setup import configure_logging, get_logger
from .tools import domain as domain_tools
from .tools import insights as insights_tools
from .tools import live as live_tools
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


async def _instrumented[T](tool: str, call: Awaitable[T]) -> T:
    """Await ``call``, recording the per-tool ok/error metric."""
    try:
        result = await call
    except Exception:
        _record_tool_call(tool, "error")
        raise
    _record_tool_call(tool, "ok")
    return result


@mcp.tool()
async def list_data_sources() -> list[dict[str, Any]]:
    """List hypertables and continuous aggregates with their time range.

    Use this first to discover what data is available before calling
    ``get_schema`` or ``query_timeseries``.
    """
    log.info("tool_invoked", tool="list_data_sources")
    return await _instrumented("list_data_sources", schema_tools.list_data_sources())


@mcp.tool()
async def get_schema(table: str) -> dict[str, Any]:
    """Describe a data source: columns, types, and JSONB key sample.

    For tables with a ``raw JSONB`` payload, returns the most common JSON keys
    observed in the latest 1000 rows so the LLM can construct
    ``raw->>'<key>'`` expressions.
    """
    log.info("tool_invoked", tool="get_schema", table=table)
    return await _instrumented("get_schema", schema_tools.get_schema(table))


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
    return await _instrumented(
        "query_timeseries",
        timeseries_tools.query_timeseries(
            table=table,
            columns=columns,
            from_ts=from_ts,
            to_ts=to_ts,
            settings=_require_settings(),
            aggregation=aggregation,  # type: ignore[arg-type]
            bucket=bucket,
            filters=filters,
        ),
    )


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
    return await _instrumented(
        "query_energy_flow",
        domain_tools.query_energy_flow(
            from_ts=from_ts, to_ts=to_ts, settings=_require_settings(), bucket=bucket
        ),
    )


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
    return await _instrumented(
        "query_heating_cycles",
        domain_tools.query_heating_cycles(
            from_ts=from_ts,
            to_ts=to_ts,
            settings=_require_settings(),
            min_duration_seconds=min_duration_seconds,
        ),
    )


@mcp.tool()
async def query_room_climate(
    room: str,
    from_ts: str,
    to_ts: str,
    bucket: str = "1 hour",
    functions: list[str] | None = None,
) -> dict[str, Any]:
    """Bucketed average reading per GA name within a room.

    ``room`` is validated against the distinct rooms in ``ga_catalog`` so
    unknown rooms produce a precise error the LLM can self-correct against.
    Optional ``functions`` narrows the result to GAs whose ETS function name
    is in the given list.
    """
    log.info("tool_invoked", tool="query_room_climate", room=room, bucket=bucket)
    return await _instrumented(
        "query_room_climate",
        domain_tools.query_room_climate(
            room=room,
            from_ts=from_ts,
            to_ts=to_ts,
            settings=_require_settings(),
            bucket=bucket,
            functions=functions,
        ),
    )


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
    """Raw KNX event log against ``ga_catalog_view``.

    All predicates AND-combined; pass none for an unfiltered window.

    * ``room``      — exact match (e.g. ``"Wohnzimmer"``)
    * ``ga``        — exact GA match (``"4/2/161"``)
    * ``name``      — substring match against ``ga_name`` (``ILIKE``);
                      pass partial words like ``"Beleuchtung"``
    * ``functions`` — list of ETS function names, validated against the
                      catalog (unknown names error with the valid list);
                      combine with ``room`` to narrow to e.g. all lighting
                      events in a room

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
    return await _instrumented(
        "query_knx_events",
        domain_tools.query_knx_events(
            from_ts=from_ts,
            to_ts=to_ts,
            settings=_require_settings(),
            room=room,
            ga=ga,
            name=name,
            functions=functions,
            limit=limit,
        ),
    )


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
    return await _instrumented(
        "query_unifi_events",
        domain_tools.query_unifi_events(
            from_ts=from_ts,
            to_ts=to_ts,
            settings=_require_settings(),
            camera=camera,
            detection_type=detection_type,
            event_type=event_type,
            min_score=min_score,
            event_id=event_id,
            limit=limit,
        ),
    )


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
    return await _instrumented(
        "correlate_events",
        domain_tools.correlate_events(
            source_a=source_a,
            source_b=source_b,
            from_ts=from_ts,
            to_ts=to_ts,
            settings=_require_settings(),
            window=window,
            bucket=bucket,
        ),
    )


@mcp.tool()
async def detect_anomalies(
    source: str | None = None,
    severity: str | None = None,
    uc: str | None = None,
    since: str = "1 day",
    until: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Anomalies flagged by the batch jobs over the recent past.

    Filters AND-combined:
    * ``source``   — source-CAGG name (e.g. ``"ems_esp_boiler_1h"``)
    * ``severity`` — one of ``"info"``, ``"warning"``, ``"critical"``
    * ``uc``       — use-case slug (e.g. ``"boiler_curburnpow"``,
                     ``"pv_production"``)
    * ``since``    — Postgres interval, default ``"1 day ago"``
    * ``until``    — optional upper bound ISO timestamp

    Newest first. ``limit`` capped at the server's ``query_row_limit``.
    """
    log.info(
        "tool_invoked",
        tool="detect_anomalies",
        source=source,
        severity=severity,
        uc=uc,
        since=since,
        limit=limit,
    )
    return await _instrumented(
        "detect_anomalies",
        insights_tools.detect_anomalies(
            settings=_require_settings(),
            source=source,
            severity=severity,
            uc=uc,
            since=since,
            until=until,
            limit=limit,
        ),
    )


@mcp.tool()
async def explain_anomaly(
    time: str,
    source: str,
    metric: str,
    detector: str,
    context_hours: int = 24,
) -> dict[str, Any]:
    """Look up a specific anomaly by its composite key and return the
    surrounding ``context_hours`` window of the same (source, metric).

    Use the four fields from a ``detect_anomalies`` result row to
    identify the anomaly.
    """
    log.info(
        "tool_invoked",
        tool="explain_anomaly",
        source=source,
        metric=metric,
        detector=detector,
        context_hours=context_hours,
    )
    return await _instrumented(
        "explain_anomaly",
        insights_tools.explain_anomaly(
            settings=_require_settings(),
            time=time,
            source=source,
            metric=metric,
            detector=detector,
            context_hours=context_hours,
        ),
    )


@mcp.tool()
async def get_forecast(
    metric: str,
    horizon_hours: int = 24,
    model: str | None = None,
) -> dict[str, Any]:
    """Stored model forecasts for ``metric`` looking ``horizon_hours`` ahead.

    Rows are produced by the ``forecast-solar`` (PV) and ``score-seasonal``
    (statsforecast) batch jobs. An empty list means no stored forecast
    covers the requested metric/window.
    """
    log.info(
        "tool_invoked",
        tool="get_forecast",
        metric=metric,
        horizon_hours=horizon_hours,
        model=model,
    )
    return await _instrumented(
        "get_forecast",
        insights_tools.get_forecast(
            settings=_require_settings(),
            metric=metric,
            horizon_hours=horizon_hours,
            model=model,
        ),
    )


@mcp.tool()
async def generate_insight_report(
    timeframe: str = "1 week",
    top_n: int = 10,
) -> dict[str, Any]:
    """Aggregate anomaly summary over ``timeframe`` for a Markdown digest.

    Returns severity counts, the top-N (uc, severity) by count, and the
    ``top_n`` most recent warning/critical entries. Compose the prose
    yourself — the tool only returns structured data.
    """
    log.info(
        "tool_invoked",
        tool="generate_insight_report",
        timeframe=timeframe,
        top_n=top_n,
    )
    return await _instrumented(
        "generate_insight_report",
        insights_tools.generate_insight_report(
            settings=_require_settings(),
            timeframe=timeframe,
            top_n=top_n,
        ),
    )


@mcp.tool()
async def get_current_state(
    domain: str,
    identifier: str | None = None,
) -> dict[str, Any]:
    """Current state of a homelab domain, read live from NATS JetStream.

    ``domain`` is one of ``knx`` | ``heating`` | ``dhw`` | ``solar`` |
    ``wallbox``. ``knx`` needs an ``identifier`` group address (``"1/2/3"``);
    ``solar`` accepts an optional inverter id (``"1"`` | ``"2"``).

    Answers "what is X doing right now?" sub-second. Returns ``status: "ok"``
    with the live ``state``, an ``as_of`` timestamp and a ``freshness_seconds``
    age. Cyclic domains (heating/dhw/solar) add ``stale: true`` once the sensor
    goes quiet; event-driven domains (knx/wallbox) report the age only — an old
    value is still current. If the subject was never seen → ``status: "unknown"``
    with ``last_known_in_tsdb`` from TimescaleDB.
    """
    log.info("tool_invoked", tool="get_current_state", domain=domain, identifier=identifier)
    return await _instrumented(
        "get_current_state",
        live_tools.get_current_state(
            domain=domain, settings=_require_settings(), identifier=identifier
        ),
    )


@mcp.tool()
async def subscribe_nats(
    subject: str,
    duration_seconds: int = 10,
) -> dict[str, Any]:
    """Tail a NATS subject for a short window and return the collected messages.

    Token-expensive — use only for "watch this for a moment" requests.
    ``subject`` must start with a known stream prefix (``knx.`` | ``ems-esp.`` |
    ``solaredge-1.`` | ``solaredge-2.`` | ``warp.``) and carry at least two
    concrete tokens before any wildcard, so ``knx.>`` is rejected but
    ``knx.1.2.3`` / ``knx.1.>`` are accepted. ``duration_seconds`` is capped at 30.
    """
    log.info(
        "tool_invoked", tool="subscribe_nats", subject=subject, duration_seconds=duration_seconds
    )
    return await _instrumented(
        "subscribe_nats",
        live_tools.subscribe_nats(
            subject=subject, settings=_require_settings(), duration_seconds=duration_seconds
        ),
    )


@mcp.tool()
async def get_current_knx(
    room: str | None = None,
    function: str | None = None,
    name: str | None = None,
    only_active: bool = False,
    limit: int = 200,
) -> dict[str, Any]:
    """Current value of KNX group addresses matching a filter, read live from NATS.

    Use this — not ``query_knx_events`` — for "is the living-room light on right
    now?" / "which lights are on?". It reads the last retained value per group
    address from JetStream, so it also covers GAs that publish only on change
    (lights, switches) and would be missing from a recent event-log window.

    Filters (optional, AND-combined) resolve GAs via the catalog: ``room``
    (exact), ``function`` (exact, e.g. ``"Beleuchtung"``), ``name`` (substring).
    ``only_active=True`` returns only GAs whose current value is "on".
    """
    log.info(
        "tool_invoked",
        tool="get_current_knx",
        room=room,
        function=function,
        name=name,
        only_active=only_active,
    )
    return await _instrumented(
        "get_current_knx",
        live_tools.get_current_knx(
            settings=_require_settings(),
            room=room,
            function=function,
            name=name,
            only_active=only_active,
            limit=limit,
        ),
    )


_settings: Settings | None = None


def _require_settings() -> Settings:
    """Return the module-level settings, raising if the app hasn't been built yet."""
    if _settings is None:
        raise RuntimeError("settings not initialised — build_app() must run first")
    return _settings


async def _livez(_request: Request) -> JSONResponse:
    # Liveness: the process is up and serving — deliberately independent of
    # DB/NATS health so an upstream outage never restarts the pod.
    return JSONResponse({"status": "alive"})


async def _healthz(_request: Request) -> JSONResponse:
    # Readiness/deep health: DB is the critical dependency and gates the
    # status code; NATS is reported for visibility but a transient blip must
    # not flap the pod (live tools only). Use /livez for liveness probes.
    db_ok = await db.healthcheck()
    body: dict[str, Any] = {"status": "ok" if db_ok else "degraded", "db": db_ok}
    if _settings is not None and _settings.nats_enabled:
        body["nats"] = await nats_module.healthcheck()
    return JSONResponse(body, status_code=200 if db_ok else 503)


async def _oauth_protected_resource(_request: Request) -> JSONResponse:
    if _settings is None:
        return JSONResponse({"error": "not_ready"}, status_code=503)
    return JSONResponse(auth_module.oauth_protected_resource_metadata(_settings))


def build_app() -> Starlette:
    """Load settings and assemble the Starlette app (uvicorn factory target)."""
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
        if _settings.nats_enabled:
            await nats_module.init(_settings)
        metrics_server, metrics_thread = metrics_module.serve(
            metrics_module.get(), _settings.metrics_port
        )
        log.info(
            "iot_mcp_bridge_ready",
            host=_settings.host,
            port=_settings.port,
            metrics_port=_settings.metrics_port,
            auth_enabled=_settings.auth_enabled,
            nats_enabled=_settings.nats_enabled,
        )
        try:
            async with mcp_app.lifespan(app):
                yield
        finally:
            metrics_server.shutdown()
            metrics_thread.join()
            await nats_module.close()
            await db.close_pool()

    routes = [
        Route("/livez", _livez, methods=["GET"]),
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
