from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from iot_mcp_bridge.config import Settings
from iot_mcp_bridge.tools import timeseries as ts

pytestmark = pytest.mark.asyncio


def _now_window() -> tuple[str, str]:
    now = datetime.now(tz=UTC)
    return (now - timedelta(hours=4)).isoformat(), (now + timedelta(minutes=1)).isoformat()


async def test_query_minute_bucket_uses_raw_hypertable(db_pool: None, settings: Settings) -> None:
    from_ts, to_ts = _now_window()
    result = await ts.query_timeseries(
        table="knx",
        columns=["value"],
        from_ts=from_ts,
        to_ts=to_ts,
        aggregation="avg",
        bucket="5 minutes",
        settings=settings,
    )
    assert result["table_used"] == "knx"
    assert result["kind_used"] == "hypertable"
    assert result["row_count"] > 0


async def test_query_hour_bucket_routes_to_cagg(db_pool: None, settings: Settings) -> None:
    from_ts, to_ts = _now_window()
    result = await ts.query_timeseries(
        table="knx",
        columns=["value"],
        from_ts=from_ts,
        to_ts=to_ts,
        aggregation="avg",
        bucket="1 hour",
        settings=settings,
    )
    assert result["table_used"] == "knx_1h"
    assert result["kind_used"] == "continuous_aggregate"


async def test_query_unknown_table_rejected(db_pool: None, settings: Settings) -> None:
    from_ts, to_ts = _now_window()
    with pytest.raises(ValueError, match="unknown_table"):
        await ts.query_timeseries(
            table="not_a_table",
            columns=["value"],
            from_ts=from_ts,
            to_ts=to_ts,
            settings=settings,
        )


async def test_query_unknown_column_rejected(db_pool: None, settings: Settings) -> None:
    from_ts, to_ts = _now_window()
    with pytest.raises(ValueError, match="unknown_columns"):
        await ts.query_timeseries(
            table="knx",
            columns=["does_not_exist"],
            from_ts=from_ts,
            to_ts=to_ts,
            settings=settings,
        )


async def test_query_invalid_bucket_rejected(db_pool: None, settings: Settings) -> None:
    from_ts, to_ts = _now_window()
    with pytest.raises(ValueError, match="invalid_bucket_interval"):
        await ts.query_timeseries(
            table="knx",
            columns=["value"],
            from_ts=from_ts,
            to_ts=to_ts,
            bucket="DROP TABLE knx",
            settings=settings,
        )


async def test_query_invalid_aggregation_rejected(db_pool: None, settings: Settings) -> None:
    from_ts, to_ts = _now_window()
    with pytest.raises(ValueError, match="invalid_aggregation"):
        await ts.query_timeseries(
            table="knx",
            columns=["value"],
            from_ts=from_ts,
            to_ts=to_ts,
            aggregation="median",  # type: ignore[arg-type]
            settings=settings,
        )


async def test_query_row_limit_enforced(db_pool: None, settings: Settings) -> None:
    settings_low = settings.model_copy(update={"query_row_limit": 2})
    from_ts, to_ts = _now_window()
    with pytest.raises(ValueError, match="row_limit_exceeded"):
        await ts.query_timeseries(
            table="knx",
            columns=["value"],
            from_ts=from_ts,
            to_ts=to_ts,
            bucket="1 minute",
            settings=settings_low,
        )
