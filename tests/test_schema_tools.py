from __future__ import annotations

import pytest

from iot_mcp_bridge.tools import schema as schema_tools

pytestmark = pytest.mark.asyncio


async def test_list_data_sources_returns_hypertables_and_caggs(db_pool: None) -> None:
    sources = await schema_tools.list_data_sources()
    names = {s["name"]: s for s in sources}

    assert "knx" in names
    assert names["knx"]["kind"] == schema_tools.KIND_HYPERTABLE
    assert names["knx"]["time_column"] == "time"
    assert names["knx"]["time_range"]["min"] is not None

    assert "ems_esp" in names
    assert names["ems_esp"]["kind"] == schema_tools.KIND_HYPERTABLE

    assert "knx_1h" in names
    assert names["knx_1h"]["kind"] == schema_tools.KIND_CONTINUOUS_AGGREGATE
    assert names["knx_1h"]["time_column"] == "bucket"


async def test_get_schema_returns_columns_and_jsonb_keys(db_pool: None) -> None:
    schema = await schema_tools.get_schema("ems_esp")

    assert schema["name"] == "ems_esp"
    assert schema["time_column"] == "time"
    col_names = {c["name"] for c in schema["columns"]}
    assert {"time", "topic", "raw"} <= col_names

    raw_keys = {entry["key"] for entry in schema["jsonb_top_keys"]["raw"]}
    assert {"flow_temp", "return_temp", "burner_power"} <= raw_keys


async def test_get_schema_unknown_table_raises(db_pool: None) -> None:
    with pytest.raises(ValueError, match="unknown_table"):
        await schema_tools.get_schema("does_not_exist")


async def test_get_schema_knx_hint_present(db_pool: None) -> None:
    schema = await schema_tools.get_schema("knx")
    assert schema["hint"] is not None
    assert "knx-nats-bridge-mapping" in schema["hint"]
