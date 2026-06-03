"""Read-only views into the anomaly + forecast tables for Claude.

These tools never insert — that's the jobs CLI's job. They exist so the
LLM can answer "what did we flag this week?" and drill into specific
entries without round-tripping to a dashboard.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ..config import Settings
from ..db import connection


def _serialize(row: dict[str, Any]) -> dict[str, Any]:
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}


VALID_SEVERITY = ("info", "warning", "critical")


async def detect_anomalies(
    *,
    settings: Settings,
    source: str | None = None,
    severity: str | None = None,
    uc: str | None = None,
    since: str = "1 day",
    until: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Recent anomaly rows, newest first. Filter by source, severity, or UC."""
    if severity and severity not in VALID_SEVERITY:
        raise ValueError(f"severity must be one of {VALID_SEVERITY}; got {severity!r}")
    limit = max(1, min(limit, settings.query_row_limit))

    where = ["time >= now() - %s::interval"]
    params: list[Any] = [since]
    if until is not None:
        where.append("time <= %s::timestamptz")
        params.append(until)
    if source:
        where.append("source = %s")
        params.append(source)
    if severity:
        where.append("severity = %s")
        params.append(severity)
    if uc:
        where.append("uc = %s")
        params.append(uc)

    query = f"""
        SELECT time, created_at, source, metric, detector, severity, uc,
               actual, expected, score, payload
        FROM mcp_anomalies
        WHERE {' AND '.join(where)}
        ORDER BY time DESC
        LIMIT %s
    """
    params.append(limit)

    async with connection() as conn, conn.cursor() as cur:
        await cur.execute(query, params)
        rows = await cur.fetchall()

    return {
        "row_count": len(rows),
        "rows": [_serialize(r) for r in rows],
    }


async def explain_anomaly(
    *,
    settings: Settings,  # noqa: ARG001 — keep signature consistent with the other tools
    time: str,
    source: str,
    metric: str,
    detector: str,
    context_hours: int = 24,
) -> dict[str, Any]:
    """Look up one anomaly and the surrounding ``context_hours`` of the
    same (source, metric) so the model can describe the trend that led
    to the flag.
    """
    context_hours = max(1, min(context_hours, 168))

    async with connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            SELECT time, created_at, source, metric, detector, severity, uc,
                   actual, expected, score, payload
            FROM mcp_anomalies
            WHERE time = %s::timestamptz AND source = %s AND metric = %s AND detector = %s
            """,
            (time, source, metric, detector),
        )
        anomaly = await cur.fetchone()

        await cur.execute(
            """
            SELECT time, source, metric, detector, severity, score, actual, expected
            FROM mcp_anomalies
            WHERE source = %s AND metric = %s
              AND time BETWEEN %s::timestamptz - make_interval(hours => %s)
                           AND %s::timestamptz + make_interval(hours => %s)
            ORDER BY time
            """,
            (source, metric, time, context_hours, time, context_hours),
        )
        context = await cur.fetchall()

    if anomaly is None:
        return {"found": False}
    return {
        "found": True,
        "anomaly": _serialize(anomaly),
        "context_hours": context_hours,
        "context": [_serialize(r) for r in context],
    }


async def get_forecast(
    *,
    settings: Settings,  # noqa: ARG001 — keep signature consistent with the other tools
    metric: str,
    horizon_hours: int = 24,
    model: str | None = None,
) -> dict[str, Any]:
    """Recent stored forecasts for ``metric`` over the next
    ``horizon_hours`` hours. Read-only — rows are populated by the
    ``forecast-solar`` (PV) and ``score-seasonal`` (statsforecast)
    CronJobs."""
    horizon_hours = max(1, min(horizon_hours, 24 * 14))
    where = [
        "metric = %s",
        "forecast_for BETWEEN now() AND now() + make_interval(hours => %s)",
    ]
    params: list[Any] = [metric, horizon_hours]
    if model:
        where.append("model = %s")
        params.append(model)

    query = f"""
        SELECT forecast_for, created_at, source, metric, model,
               forecast_value, forecast_lower, forecast_upper
        FROM mcp_forecasts
        WHERE {' AND '.join(where)}
        ORDER BY forecast_for, model
    """

    async with connection() as conn, conn.cursor() as cur:
        await cur.execute(query, params)
        rows = await cur.fetchall()

    return {
        "metric": metric,
        "horizon_hours": horizon_hours,
        "row_count": len(rows),
        "rows": [_serialize(r) for r in rows],
    }


async def generate_insight_report(
    *,
    settings: Settings,  # noqa: ARG001 — keep signature consistent with the other tools
    timeframe: str = "1 week",
    top_n: int = 10,
) -> dict[str, Any]:
    """Aggregate summary of anomalies over ``timeframe``.

    Returns severity counts, top-N (uc, severity) pairs by count, and
    the ``top_n`` most recent critical/warning entries. The MCP layer
    hands this dict to the LLM, which composes a Markdown response.
    """
    top_n = max(1, min(top_n, 50))

    async with connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            SELECT severity, count(*) AS cnt
            FROM mcp_anomalies
            WHERE time >= now() - %s::interval
            GROUP BY severity
            """,
            (timeframe,),
        )
        severity_counts = {r["severity"]: r["cnt"] for r in await cur.fetchall()}

        await cur.execute(
            """
            SELECT uc, severity, count(*) AS cnt
            FROM mcp_anomalies
            WHERE time >= now() - %s::interval AND uc IS NOT NULL
            GROUP BY uc, severity
            ORDER BY cnt DESC
            LIMIT %s
            """,
            (timeframe, top_n),
        )
        top_ucs = [_serialize(r) for r in await cur.fetchall()]

        await cur.execute(
            """
            SELECT time, source, metric, severity, uc, score, actual, expected
            FROM mcp_anomalies
            WHERE time >= now() - %s::interval
              AND severity IN ('warning', 'critical')
            ORDER BY time DESC
            LIMIT %s
            """,
            (timeframe, top_n),
        )
        recent_serious = [_serialize(r) for r in await cur.fetchall()]

    return {
        "timeframe": timeframe,
        "severity_counts": severity_counts,
        "top_ucs": top_ucs,
        "recent_serious": recent_serious,
    }
