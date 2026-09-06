"""App-factory tests: probe endpoints, OAuth discovery, MCP tool registration.

``build_app()`` is exercised with the real seeded TimescaleDB container behind
it (NATS disabled, auth disabled); the MCP layer is driven in-process via the
fastmcp ``Client`` so the ``@mcp.tool`` wrappers and their metrics
instrumentation run for real.
"""

from __future__ import annotations

import pytest
from fastmcp import Client
from starlette.testclient import TestClient
from testcontainers.postgres import PostgresContainer

from iot_mcp_bridge import server

EXPECTED_TOOLS = {
    "list_data_sources",
    "get_schema",
    "query_timeseries",
    "query_energy_flow",
    "query_heating_cycles",
    "query_room_climate",
    "query_knx_events",
    "query_unifi_events",
    "correlate_events",
    "get_forecast",
    "get_pv_forecast",
    "get_weather_forecast",
    "get_current_state",
    "subscribe_nats",
    "get_current_knx",
}


@pytest.fixture
def app_env(timescaledb_container: PostgresContainer, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MCP_DB_HOST", timescaledb_container.get_container_host_ip())
    monkeypatch.setenv("MCP_DB_PORT", str(timescaledb_container.get_exposed_port(5432)))
    monkeypatch.setenv("MCP_DB_NAME", "homelab")
    monkeypatch.setenv("MCP_DB_USERNAME", "test")
    monkeypatch.setenv("MCP_DB_PASSWORD", "test")
    monkeypatch.setenv("MCP_AUTH_ENABLED", "false")
    monkeypatch.setenv("MCP_NATS_ENABLED", "false")
    monkeypatch.setenv("MCP_METRICS_PORT", "0")  # ephemeral port, avoids clashes


def test_health_and_discovery_endpoints(app_env: None) -> None:
    app = server.build_app()
    with TestClient(app) as client:
        livez = client.get("/livez")
        assert livez.status_code == 200
        assert livez.json() == {"status": "alive"}

        healthz = client.get("/healthz")
        assert healthz.status_code == 200
        body = healthz.json()
        assert body["status"] == "ok"
        assert body["db"] is True
        assert "nats" not in body  # disabled → not reported

        meta = client.get("/.well-known/oauth-protected-resource")
        assert meta.status_code == 200
        assert "authorization_servers" in meta.json()


async def test_all_tools_registered() -> None:
    async with Client(server.mcp) as client:
        tools = await client.list_tools()
    assert {t.name for t in tools} == EXPECTED_TOOLS


async def test_tool_call_roundtrip(db_pool: None) -> None:
    """list_data_sources through the MCP layer returns the seeded hypertables."""
    async with Client(server.mcp) as client:
        result = await client.call_tool("list_data_sources", {})
    names = {entry["name"] for entry in result.data}
    assert {"knx", "ems_esp"} <= names
