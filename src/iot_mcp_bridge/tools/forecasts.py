"""Ergonomic, forecast-specific read tools for Claude (Phase 4).

These are thin specialisations of the generic ``insights.get_forecast``
read: they pin the ``metric``/``model`` an LLM would otherwise have to
guess, and reshape the output for easy consumption. All data is produced
by batch jobs (forecast-solar, forecast-weather) — these tools never call
an external API themselves.
"""

from __future__ import annotations

from typing import Any

from ..config import Settings
from . import insights

# forecast-solar CronJob writes PV production (watts) under this triple.
PV_METRIC = "pv_production"
PV_MODEL = "forecast_solar"


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
