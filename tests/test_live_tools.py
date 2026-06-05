"""Live-tool tests.

The JetStream layer (``iot_mcp_bridge.nats``) is monkeypatched — these are unit
tests of the domain→subject mapping, field extraction, freshness logic and the
subscribe allowlist. The unknown path hits the real seeded TimescaleDB via the
``db_pool`` fixture to exercise the ``last_known_in_tsdb`` fallback.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from iot_mcp_bridge import nats as nats_module
from iot_mcp_bridge.config import Settings
from iot_mcp_bridge.nats import StateMsg
from iot_mcp_bridge.tools import live


def _msg(subject: str, payload: Any, ts: datetime | None = None) -> StateMsg:
    return StateMsg(subject=subject, data=json.dumps(payload).encode(), ts=ts or datetime.now(UTC))


def _patch_last_msg(
    monkeypatch: pytest.MonkeyPatch, mapping: dict[str, StateMsg | None]
) -> list[str]:
    """Patch nats.last_msg from a subject→msg mapping; return captured subjects."""
    seen: list[str] = []

    async def _fake(stream: str, subject: str) -> StateMsg | None:
        seen.append(subject)
        return mapping.get(subject)

    monkeypatch.setattr(nats_module, "last_msg", _fake)
    return seen


def _patch_tail(monkeypatch: pytest.MonkeyPatch, factory: Callable[[str], list[StateMsg]]) -> None:
    async def _fake(subject: str, duration_s: float, max_messages: int = 200) -> list[StateMsg]:
        return factory(subject)

    monkeypatch.setattr(nats_module, "tail", _fake)


# =====================================================================
# get_current_state — happy paths (no DB needed)
# =====================================================================


async def test_heating_ok_fresh_not_stale(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_last_msg(
        monkeypatch,
        {
            "ems-esp.boiler_data": _msg(
                "ems-esp.boiler_data",
                {
                    "curflowtemp": 42.5,
                    "rettemp": 38.0,
                    "curburnpow": 60,
                    "pumpstatus": 1,
                    "selflowtemp": 45,
                },
            )
        },
    )
    out = await live.get_current_state("heating", settings)
    assert out["status"] == "ok"
    assert out["state"]["flow_temp"] == 42.5
    assert out["state"]["burner_power"] == 60
    assert out["state"]["target_flow_temp"] == 45
    assert out["freshness_seconds"] < 5
    assert out["stale"] is False  # cyclic domain, fresh message


async def test_dhw_merges_two_subjects(settings: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
    # storage temp / setpoint live on boiler_data_dhw; tap-water flag on boiler_data.
    _patch_last_msg(
        monkeypatch,
        {
            "ems-esp.boiler_data_dhw": _msg(
                "ems-esp.boiler_data_dhw", {"storagetemp1": 51.2, "seltemp": 55}
            ),
            "ems-esp.boiler_data": _msg("ems-esp.boiler_data", {"tapwateractive": 1, "circ": 0}),
        },
    )
    out = await live.get_current_state("dhw", settings)
    assert out["status"] == "ok"
    assert out["state"]["dhw_storage_temp"] == 51.2  # from boiler_data_dhw
    assert out["state"]["dhw_setpoint"] == 55  # from boiler_data_dhw
    assert out["state"]["dhw_active"] == 1  # from boiler_data


async def test_knx_ok_builds_dotted_subject(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen = _patch_last_msg(
        monkeypatch,
        {
            "knx.1.2.3": _msg(
                "knx.1.2.3", {"ga": "1/2/3", "name": "LR Temp", "dpt": "9.001", "value": 21.5}
            )
        },
    )
    out = await live.get_current_state("knx", settings, identifier="1/2/3")
    assert out["status"] == "ok"
    assert out["state"]["value"] == 21.5
    assert out["state"]["name"] == "LR Temp"
    assert seen == ["knx.1.2.3"]
    assert "stale" not in out  # knx is event-driven — no stale flag


async def test_solar_ok_sums_both_inverters(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_last_msg(
        monkeypatch,
        {
            "solaredge-1.powerflow": _msg(
                "solaredge-1.powerflow",
                {"pv_production": 1000, "grid": {"power": -200}, "consumer": {"house": 800}},
            ),
            "solaredge-2.powerflow": _msg(
                "solaredge-2.powerflow", {"pv_production": 500, "grid": {"power": -100}}
            ),
        },
    )
    out = await live.get_current_state("solar", settings)
    assert out["status"] == "ok"
    assert out["state"]["pv_power"] == 1500
    assert set(out["state"]["inverters"]) == {"1", "2"}
    assert out["state"]["inverters"]["1"]["grid_power"] == -200


async def test_wallbox_state_and_power_with_separate_ages(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Charger state is days old (event-driven), power is fresh (cyclic).
    old = datetime.now(UTC) - timedelta(days=3)
    _patch_last_msg(
        monkeypatch,
        {
            "warp.evse.state": _msg(
                "warp.evse.state",
                {"charger_state": 2, "iec61851_state": 1, "allowed_charging_current": 16000},
                ts=old,
            ),
            "warp.meters.0.values": _msg(
                "warp.meters.0.values", [230, 230, 230, 5, 5, 5, 1100, 1100, 1100]
            ),
        },
    )
    out = await live.get_current_state("wallbox", settings)
    assert out["status"] == "ok"
    assert "stale" not in out  # wallbox is event-driven
    assert out["state"]["charger_state"] == 2
    assert out["state"]["allowed_charging_current"] == 16000
    assert out["state"]["power_w"] == 3300
    assert out["state"]["state_age_seconds"] > 200000  # ~3 days
    assert out["state"]["power_age_seconds"] < 5  # fresh


# =====================================================================
# Freshness semantics
# =====================================================================


async def test_cyclic_domain_flags_stale_but_keeps_value(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    old = datetime.now(UTC) - timedelta(hours=1)  # > live_stale_seconds (600s)
    _patch_last_msg(
        monkeypatch,
        {"ems-esp.boiler_data": _msg("ems-esp.boiler_data", {"curflowtemp": 40}, ts=old)},
    )
    out = await live.get_current_state("heating", settings)
    assert out["status"] == "ok"  # still ok — JetStream keeps the value
    assert out["stale"] is True
    assert out["state"]["flow_temp"] == 40
    assert "last_known_in_tsdb" not in out  # fallback is only for the unknown path


async def test_event_driven_old_value_is_not_stale(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    old = datetime.now(UTC) - timedelta(days=2)
    _patch_last_msg(
        monkeypatch,
        {"knx.1.2.2": _msg("knx.1.2.2", {"ga": "1/2/2", "value": False}, ts=old)},
    )
    out = await live.get_current_state("knx", settings, identifier="1/2/2")
    assert out["status"] == "ok"
    assert "stale" not in out  # a light switched off 2 days ago is still off
    assert out["state"]["value"] is False
    assert out["freshness_seconds"] > 100000


# =====================================================================
# get_current_state — unknown path (real seeded DB fallback)
# =====================================================================


async def test_unknown_falls_back_to_tsdb(
    settings: Settings, db_pool: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_last_msg(monkeypatch, {"ems-esp.boiler_data": None})
    out = await live.get_current_state("heating", settings)
    assert out["status"] == "unknown"
    assert out["last_known_in_tsdb"] is not None
    assert "time" in out["last_known_in_tsdb"]


async def test_knx_unknown_fallback_uses_identifier(
    settings: Settings, db_pool: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_last_msg(monkeypatch, {"knx.1.2.3": None})
    out = await live.get_current_state("knx", settings, identifier="1/2/3")
    assert out["status"] == "unknown"
    assert out["last_known_in_tsdb"] is not None
    assert out["last_known_in_tsdb"]["ga"] == "1/2/3"


# =====================================================================
# get_current_state — validation
# =====================================================================


async def test_unknown_domain_rejected(settings: Settings) -> None:
    with pytest.raises(ValueError, match="unknown_domain"):
        await live.get_current_state("furnace", settings)


async def test_knx_requires_identifier(settings: Settings) -> None:
    with pytest.raises(ValueError, match="knx_requires_identifier"):
        await live.get_current_state("knx", settings)


async def test_knx_invalid_ga_rejected(settings: Settings) -> None:
    with pytest.raises(ValueError, match="invalid_ga"):
        await live.get_current_state("knx", settings, identifier="not-a-ga")


# =====================================================================
# subscribe_nats
# =====================================================================


async def test_subscribe_rejects_top_level_wildcard(settings: Settings) -> None:
    with pytest.raises(ValueError, match="wildcard_too_broad"):
        await live.subscribe_nats("knx.>", settings, duration_seconds=5)


async def test_subscribe_rejects_unknown_prefix(settings: Settings) -> None:
    with pytest.raises(ValueError, match="subject_not_allowed"):
        await live.subscribe_nats("foo.bar.baz", settings, duration_seconds=5)


async def test_subscribe_rejects_over_cap_duration(settings: Settings) -> None:
    with pytest.raises(ValueError, match="duration_too_long"):
        await live.subscribe_nats("knx.1.2.3", settings, duration_seconds=31)


async def test_subscribe_accepts_sub_wildcard_and_collects(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_tail(
        monkeypatch,
        lambda _subject: [
            _msg("knx.1.2.3", {"value": True}),
            _msg("knx.1.2.4", {"value": 21.0}),
        ],
    )
    out = await live.subscribe_nats("knx.1.>", settings, duration_seconds=5)
    assert out["message_count"] == 2
    assert out["messages"][0]["data"] == {"value": True}
    assert out["messages"][0]["subject"] == "knx.1.2.3"


# =====================================================================
# get_current_knx — "what's on right now?" (catalog join + live values)
# =====================================================================


async def test_get_current_knx_only_active(
    settings: Settings, db_pool: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Seeded catalog: 1/2/2 = Lighting Bedroom, 1/2/100 = Lighting LivingRoom.
    _patch_last_msg(
        monkeypatch,
        {
            "knx.1.2.2": _msg("knx.1.2.2", {"value": True}),  # bedroom ceiling ON
            "knx.1.2.100": _msg("knx.1.2.100", {"value": False}),  # living-room ceiling OFF
        },
    )
    out = await live.get_current_knx(settings, function="Lighting", only_active=True)
    assert out["count"] == 1
    assert out["states"][0]["ga"] == "1/2/2"
    assert out["states"][0]["value"] is True
    assert out["states"][0]["room"] == "Bedroom"
    assert out["states"][0]["age_seconds"] < 5


async def test_get_current_knx_lists_all_matching(
    settings: Settings, db_pool: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_last_msg(
        monkeypatch,
        {
            "knx.1.2.2": _msg("knx.1.2.2", {"value": True}),
            "knx.1.2.100": _msg("knx.1.2.100", {"value": False}),
        },
    )
    out = await live.get_current_knx(settings, function="Lighting")
    assert out["count"] == 2
    assert {s["ga"] for s in out["states"]} == {"1/2/2", "1/2/100"}


async def test_get_current_knx_omits_gas_without_nats(
    settings: Settings, db_pool: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_last_msg(monkeypatch, {"knx.1.2.2": _msg("knx.1.2.2", {"value": True})})
    out = await live.get_current_knx(settings, function="Lighting")
    assert out["count"] == 1  # 1/2/100 has no retained NATS message → omitted
    assert out["states"][0]["ga"] == "1/2/2"


async def test_get_current_knx_no_catalog_match(
    settings: Settings, db_pool: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_last_msg(monkeypatch, {})
    out = await live.get_current_knx(settings, room="Nonexistent")
    assert out["count"] == 0
    assert out["states"] == []
