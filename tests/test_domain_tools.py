from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from iot_mcp_bridge.config import Settings
from iot_mcp_bridge.tools import domain as domain_tools

# Use a generous future-window to ensure no rows match for "empty range" tests.
_FAR_FUTURE = "2099-01-01T00:00:00Z"
_FAR_FUTURE_END = "2099-12-31T00:00:00Z"


def _now_window() -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    return (
        (now - timedelta(days=30)).isoformat(),
        (now + timedelta(hours=1)).isoformat(),
    )


# =====================================================================
# query_energy_flow
# =====================================================================


async def test_energy_flow_happy_path(settings: Settings, db_pool: None) -> None:
    f, t = _now_window()
    out = await domain_tools.query_energy_flow(f, t, settings=settings, bucket="1 hour")
    assert out["row_count"] > 0
    sample = out["rows"][0]
    assert "bucket" in sample
    assert "pv_production_avg" in sample
    assert "evcharger_power_avg" in sample


async def test_energy_flow_empty_range(settings: Settings, db_pool: None) -> None:
    out = await domain_tools.query_energy_flow(
        _FAR_FUTURE, _FAR_FUTURE_END, settings=settings, bucket="1 day"
    )
    assert out["row_count"] == 0


async def test_energy_flow_rejects_sub_hour_bucket(settings: Settings, db_pool: None) -> None:
    f, t = _now_window()
    with pytest.raises(ValueError, match="invalid_bucket"):
        await domain_tools.query_energy_flow(f, t, settings=settings, bucket="15 minutes")


# =====================================================================
# query_heating_cycles
# =====================================================================


async def test_heating_cycles_detects_runs(settings: Settings, db_pool: None) -> None:
    f, t = _now_window()
    out = await domain_tools.query_heating_cycles(
        f, t, settings=settings, min_duration_seconds=0
    )
    # Seed pattern: every 3rd minute burns. Over 200 rows that's ~67 ON-states,
    # each typically a single-row cycle, but cycle merging may collapse them
    # depending on exact timing — at minimum we should see SOME cycles.
    assert out["cycle_count"] >= 30
    assert out["total_runtime_seconds"] >= 0
    cycle = out["cycles"][0]
    assert {"start_ts", "end_ts", "duration_seconds", "peak_power_kw", "avg_power_kw"} <= cycle.keys()


async def test_heating_cycles_min_duration_filter(settings: Settings, db_pool: None) -> None:
    f, t = _now_window()
    # Force a high floor — most single-minute cycles drop out.
    out = await domain_tools.query_heating_cycles(
        f, t, settings=settings, min_duration_seconds=3600
    )
    assert all(c["duration_seconds"] >= 3600 for c in out["cycles"])


async def test_heating_cycles_rejects_negative_min_duration(
    settings: Settings, db_pool: None
) -> None:
    f, t = _now_window()
    with pytest.raises(ValueError, match="invalid_min_duration_seconds"):
        await domain_tools.query_heating_cycles(
            f, t, settings=settings, min_duration_seconds=-5
        )


# =====================================================================
# query_room_climate
# =====================================================================


async def test_room_climate_happy_path(settings: Settings, db_pool: None) -> None:
    f, t = _now_window()
    out = await domain_tools.query_room_climate(
        "Bedroom", f, t, settings=settings, bucket="1 hour"
    )
    assert out["row_count"] > 0
    assert out["room"] == "Bedroom"
    sample = out["rows"][0]
    assert "ga_name" in sample
    assert "value_avg" in sample


async def test_room_climate_function_filter(settings: Settings, db_pool: None) -> None:
    f, t = _now_window()
    # When `functions` is given, only GAs with those function names come back.
    out = await domain_tools.query_room_climate(
        "Bedroom",
        f,
        t,
        settings=settings,
        bucket="1 hour",
        functions=["Climate", "Sensors"],
    )
    # Bedroom has 3 GAs total but the Lighting one must be filtered out.
    ga_names = {r["ga_name"] for r in out["rows"]}
    assert all("Lighting" not in n for n in ga_names)


async def test_room_climate_unknown_room_lists_valid(
    settings: Settings, db_pool: None
) -> None:
    f, t = _now_window()
    with pytest.raises(ValueError) as exc_info:
        await domain_tools.query_room_climate(
            "Attic", f, t, settings=settings, bucket="1 hour"
        )
    msg = str(exc_info.value)
    assert "unknown_room" in msg
    # Error includes the valid choices so the LLM can self-correct.
    assert "Bedroom" in msg
    assert "LivingRoom" in msg


async def test_room_climate_rejects_bad_bucket(settings: Settings, db_pool: None) -> None:
    f, t = _now_window()
    with pytest.raises(ValueError, match="invalid_bucket_interval"):
        await domain_tools.query_room_climate(
            "Bedroom", f, t, settings=settings, bucket="not-a-bucket"
        )


# =====================================================================
# query_knx_events
# =====================================================================


async def test_knx_events_happy_path(settings: Settings, db_pool: None) -> None:
    f, t = _now_window()
    out = await domain_tools.query_knx_events(f, t, settings=settings, limit=500)
    assert out["row_count"] > 0
    sample = out["rows"][0]
    assert {"time", "ga", "ga_name"} <= sample.keys()


async def test_knx_events_room_filter(settings: Settings, db_pool: None) -> None:
    f, t = _now_window()
    out = await domain_tools.query_knx_events(
        f, t, settings=settings, room="Bedroom", limit=500
    )
    assert all(r["room"] == "Bedroom" for r in out["rows"])


async def test_knx_events_name_filter(settings: Settings, db_pool: None) -> None:
    f, t = _now_window()
    out = await domain_tools.query_knx_events(
        f, t, settings=settings, name="Temperature", limit=500
    )
    # Catalog has Temperature entries in both rooms (1/2/0 and 1/2/3).
    assert all("Temperature" in (r["ga_name"] or "") for r in out["rows"])


async def test_knx_events_rejects_zero_limit(settings: Settings, db_pool: None) -> None:
    f, t = _now_window()
    with pytest.raises(ValueError, match="invalid_limit"):
        await domain_tools.query_knx_events(f, t, settings=settings, limit=0)


# =====================================================================
# correlate_events
# =====================================================================


async def test_correlate_events_finds_pv_wallbox_lag(
    settings: Settings, db_pool: None
) -> None:
    f, t = _now_window()
    out = await domain_tools.correlate_events(
        # Bucket >= 1h auto-routes to *_1h CAGGs, so use CAGG-side column names.
        source_a={"table": "solaredge_powerflow", "column": "pv_production_avg"},
        source_b={"table": "warp_meter", "column": "power_total_avg"},
        from_ts=f,
        to_ts=t,
        settings=settings,
        window="3 hours",
        bucket="1 hour",
    )
    assert out["bucket"] == "1 hour"
    assert out["window"] == "3 hours"
    assert out["n_lags"] == 3
    # PV peaks at hour 12, wallbox at hour 14 (offset 2h) — the best lag should
    # be in the +/-2h range. The actual sign depends on which series is "a".
    assert out["best"] is not None
    assert abs(out["best"]["lag_buckets"]) <= 3


async def test_correlate_events_rejects_window_smaller_than_bucket(
    settings: Settings, db_pool: None
) -> None:
    f, t = _now_window()
    with pytest.raises(ValueError, match="window must be larger than bucket"):
        await domain_tools.correlate_events(
            source_a={"table": "solaredge_powerflow", "column": "pv_production_avg"},
            source_b={"table": "warp_meter", "column": "power_total_avg"},
            from_ts=f,
            to_ts=t,
            settings=settings,
            window="1 hour",
            bucket="1 day",
        )


async def test_correlate_events_unknown_table(settings: Settings, db_pool: None) -> None:
    f, t = _now_window()
    with pytest.raises(ValueError, match="unknown_table"):
        await domain_tools.correlate_events(
            source_a={"table": "does_not_exist", "column": "x"},
            source_b={"table": "warp_meter", "column": "power_total_avg"},
            from_ts=f,
            to_ts=t,
            settings=settings,
            window="3 hours",
            bucket="1 hour",
        )
