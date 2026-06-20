"""Ergonomic, forecast-specific read tools for Claude (Phase 4).

These are thin specialisations of the generic ``insights.get_forecast``
read: they pin the ``metric``/``model`` an LLM would otherwise have to
guess, and reshape the output for easy consumption. All data is produced
by batch jobs (forecast-solar, forecast-weather) — these tools never call
an external API themselves.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ..config import Settings
from ..db import connection
from . import insights

# forecast-solar CronJob writes PV production (watts) under this triple.
PV_METRIC = "pv_production"
PV_MODEL = "forecast_solar"

# forecast-weather CronJob writes one row per (hour, metric) under this model.
WEATHER_MODEL = "open_meteo"
_MAX_HORIZON_HOURS = 24 * 14


async def get_pv_forecast(*, settings: Settings, hours: int = 48) -> dict[str, Any]:
    """Hour-by-hour PV production forecast (watts) for the next ``hours``.

    Specialises ``get_forecast`` to the forecast.solar rows so the caller
    need not know the metric/model strings. When the forecast-solar job
    hasn't populated the requested window, ``note`` flags it instead of
    silently returning an empty list.
    """
    result = await insights.get_forecast(
        settings=settings,
        metric=PV_METRIC,
        model=PV_MODEL,
        horizon_hours=hours,
    )
    if result["row_count"] == 0:
        result["note"] = "PV forecast not available — check forecast-solar CronJob health"
    return result


async def get_weather_forecast(
    *,
    settings: Settings,  # noqa: ARG001 — keep signature consistent with the other tools
    hours: int = 48,
) -> dict[str, Any]:
    """Hour-by-hour weather forecast for the next ``hours``, pivoted so each
    hour is one dict with the available metrics (``temperature``,
    ``cloud_cover``, ``precipitation``, ``solar_radiation``, ``wind_speed``).

    Reads the Open-Meteo (DWD ICON) rows written by the forecast-weather job
    — no live API call. When the job hasn't populated the requested window,
    ``note`` flags it instead of returning a silent empty list.
    """
    hours = max(1, min(hours, _MAX_HORIZON_HOURS))
    query = """
        SELECT forecast_for, metric, forecast_value
        FROM mcp_forecasts
        WHERE model = %s
          AND forecast_for BETWEEN now() AND now() + make_interval(hours => %s)
        ORDER BY forecast_for
    """
    async with connection() as conn, conn.cursor() as cur:
        await cur.execute(query, (WEATHER_MODEL, hours))
        rows = await cur.fetchall()

    # Pivot per-metric rows into one dict per hour. Rows arrive ordered by
    # forecast_for, so insertion order keeps the hours chronological.
    buckets: dict[datetime, dict[str, Any]] = {}
    for row in rows:
        bucket = buckets.setdefault(
            row["forecast_for"],
            {"forecast_for": row["forecast_for"].isoformat()},
        )
        bucket[row["metric"]] = row["forecast_value"]
    hourly = list(buckets.values())

    result: dict[str, Any] = {
        "model": WEATHER_MODEL,
        "horizon_hours": hours,
        "row_count": len(hourly),
        "hours": hourly,
    }
    if not hourly:
        result["note"] = "weather forecast not available — check forecast-weather CronJob health"
    return result
