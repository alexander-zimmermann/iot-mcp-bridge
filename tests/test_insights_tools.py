"""Tests for the read-only insights tools (detect_anomalies, explain_anomaly,
get_forecast, generate_insight_report)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import psycopg
import pytest
import pytest_asyncio
from psycopg.rows import dict_row

from iot_mcp_bridge.config import Settings
from iot_mcp_bridge.tools import insights


@pytest_asyncio.fixture
async def seeded_anomalies(settings: Settings, db_pool: None) -> AsyncIterator[None]:
    """Seed 10 mcp_anomalies + 4 mcp_forecasts rows with known characteristics:
    5 info / 3 warning / 2 critical across two sources, ucs, metrics."""
    conn = psycopg.connect(settings.db_dsn, autocommit=True, row_factory=dict_row)
    try:
        conn.execute("TRUNCATE TABLE mcp_anomalies")
        conn.execute("TRUNCATE TABLE mcp_forecasts")
        conn.execute(
            """
            INSERT INTO mcp_anomalies (time, source, metric, detector, severity, uc,
                                        actual, expected, score, payload)
            SELECT
                NOW() - (i || ' minutes')::interval,
                CASE WHEN i % 2 = 0 THEN 'ems_esp_boiler_1h' ELSE 'solaredge_powerflow_1h' END,
                CASE WHEN i % 2 = 0 THEN 'curburnpow_avg' ELSE 'pv_production_avg' END,
                'zscore',
                (ARRAY['info','info','info','info','info','warning','warning','warning','critical','critical'])[i+1],
                CASE WHEN i % 2 = 0 THEN 'boiler_curburnpow' ELSE 'pv_production' END,
                100.0 + i,
                50.0,
                3.0 + i * 0.5,
                jsonb_build_object('seed', true, 'idx', i)
            FROM generate_series(0, 9) AS i
            """
        )
        conn.execute(
            """
            INSERT INTO mcp_forecasts (forecast_for, source, metric, model,
                                        forecast_value, forecast_lower, forecast_upper)
            SELECT
                NOW() + ((i + 1) || ' hours')::interval,
                'solaredge_powerflow_1h',
                'pv_production_avg',
                'mstl',
                4000.0 + i * 100,
                3500.0 + i * 100,
                4500.0 + i * 100
            FROM generate_series(0, 3) AS i
            """
        )
    finally:
        conn.close()
    yield
    conn = psycopg.connect(settings.db_dsn, autocommit=True)
    try:
        conn.execute("TRUNCATE TABLE mcp_anomalies")
        conn.execute("TRUNCATE TABLE mcp_forecasts")
    finally:
        conn.close()


# =====================================================================
# detect_anomalies
# =====================================================================


async def test_detect_anomalies_returns_all_recent(
    settings: Settings, seeded_anomalies: None
) -> None:
    result = await insights.detect_anomalies(settings=settings, since="1 day")
    assert result["row_count"] == 10
    assert {r["severity"] for r in result["rows"]} == {"info", "warning", "critical"}


async def test_detect_anomalies_filter_by_severity(
    settings: Settings, seeded_anomalies: None
) -> None:
    result = await insights.detect_anomalies(settings=settings, severity="critical", since="1 day")
    assert result["row_count"] == 2
    assert all(r["severity"] == "critical" for r in result["rows"])


async def test_detect_anomalies_filter_by_source_and_uc(
    settings: Settings, seeded_anomalies: None
) -> None:
    result = await insights.detect_anomalies(
        settings=settings,
        source="ems_esp_boiler_1h",
        uc="boiler_curburnpow",
        since="1 day",
    )
    assert result["row_count"] == 5
    assert all(r["source"] == "ems_esp_boiler_1h" for r in result["rows"])


async def test_detect_anomalies_rejects_invalid_severity(settings: Settings) -> None:
    with pytest.raises(ValueError, match="severity"):
        await insights.detect_anomalies(settings=settings, severity="bogus")


async def test_detect_anomalies_limit_capped(settings: Settings, seeded_anomalies: None) -> None:
    result = await insights.detect_anomalies(settings=settings, limit=3, since="1 day")
    assert result["row_count"] == 3


# =====================================================================
# explain_anomaly
# =====================================================================


async def test_explain_anomaly_returns_row_and_context(
    settings: Settings, seeded_anomalies: None
) -> None:
    listing = await insights.detect_anomalies(settings=settings, severity="critical", since="1 day")
    target = listing["rows"][0]
    result = await insights.explain_anomaly(
        settings=settings,
        time=target["time"],
        source=target["source"],
        metric=target["metric"],
        detector=target["detector"],
    )
    assert result["found"] is True
    assert result["anomaly"]["severity"] == "critical"
    assert len(result["context"]) >= 1


async def test_explain_anomaly_not_found(settings: Settings, db_pool: None) -> None:
    result = await insights.explain_anomaly(
        settings=settings,
        time="1970-01-01T00:00:00Z",
        source="ems_esp_boiler_1h",
        metric="curburnpow_avg",
        detector="zscore",
    )
    assert result["found"] is False


# =====================================================================
# get_forecast
# =====================================================================


async def test_get_forecast_returns_seeded_rows(settings: Settings, seeded_anomalies: None) -> None:
    result = await insights.get_forecast(settings=settings, metric="pv_production_avg")
    assert result["metric"] == "pv_production_avg"
    assert result["row_count"] == 4


async def test_get_forecast_unknown_metric_empty(
    settings: Settings, seeded_anomalies: None
) -> None:
    result = await insights.get_forecast(settings=settings, metric="does_not_exist")
    assert result["row_count"] == 0


# =====================================================================
# generate_insight_report
# =====================================================================


async def test_generate_insight_report_aggregates(
    settings: Settings, seeded_anomalies: None
) -> None:
    result = await insights.generate_insight_report(settings=settings, timeframe="1 day")
    assert result["severity_counts"]["info"] == 5
    assert result["severity_counts"]["warning"] == 3
    assert result["severity_counts"]["critical"] == 2
    # 6 (uc, severity) pairs: 2 ucs × 3 severities.
    assert len(result["top_ucs"]) == 6
    # 3 warning + 2 critical = 5 serious.
    assert len(result["recent_serious"]) == 5
