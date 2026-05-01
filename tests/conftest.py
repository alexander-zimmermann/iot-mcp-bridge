from __future__ import annotations

import os
from collections.abc import AsyncIterator

import psycopg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from iot_mcp_bridge import db
from iot_mcp_bridge.config import Settings

# TimescaleDB image with the extension preinstalled.
TIMESCALEDB_IMAGE = "timescale/timescaledb:latest-pg17"


@pytest.fixture(scope="session")
def timescaledb_container() -> PostgresContainer:
    container = PostgresContainer(
        TIMESCALEDB_IMAGE, username="test", password="test", dbname="homelab"
    )
    # TimescaleDB needs the extension loaded; the official image has it as a shared lib
    # but autoloads only when the extension is created in the database.
    container.start()
    try:
        with psycopg.connect(
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(5432)),
            user="test",
            password="test",
            dbname="homelab",
            autocommit=True,
        ) as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            _seed(conn)
        yield container
    finally:
        container.stop()


def _seed(conn: psycopg.Connection) -> None:
    """Set up two hypertables + one continuous aggregate with a few rows."""
    conn.execute(
        """
        CREATE TABLE knx (
            time TIMESTAMPTZ NOT NULL,
            ga   TEXT NOT NULL,
            dpt  TEXT,
            value DOUBLE PRECISION,
            raw  JSONB
        );
        SELECT create_hypertable('knx', by_range('time'));

        CREATE TABLE ems_esp (
            time TIMESTAMPTZ NOT NULL,
            topic TEXT NOT NULL,
            raw  JSONB NOT NULL
        );
        SELECT create_hypertable('ems_esp', by_range('time'));

        INSERT INTO knx
        SELECT
            NOW() - (i || ' minutes')::interval,
            '1/2/' || (i % 5),
            '9.001',
            20 + (i % 10),
            jsonb_build_object('value', 20 + (i % 10), 'unit', 'celsius')
        FROM generate_series(0, 200) AS i;

        INSERT INTO ems_esp
        SELECT
            NOW() - (i || ' minutes')::interval,
            'boiler_data',
            jsonb_build_object(
                'flow_temp', 35 + (i % 20),
                'return_temp', 30 + (i % 15),
                'burner_power', CASE WHEN i % 3 = 0 THEN 50 ELSE 0 END
            )
        FROM generate_series(0, 200) AS i;
        """
    )

    conn.execute(
        """
        CREATE MATERIALIZED VIEW knx_1h
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 hour', time) AS bucket,
            ga,
            avg(value)  AS value,
            count(*)    AS samples
        FROM knx
        GROUP BY bucket, ga
        WITH NO DATA;
        SELECT add_continuous_aggregate_policy(
            'knx_1h',
            start_offset => INTERVAL '7 days',
            end_offset   => INTERVAL '5 minutes',
            schedule_interval => INTERVAL '1 hour'
        );
        CALL refresh_continuous_aggregate('knx_1h', NULL, NULL);
        """
    )


@pytest.fixture
def settings(timescaledb_container: PostgresContainer) -> Settings:
    os.environ.update(
        MCP_DB_HOST=timescaledb_container.get_container_host_ip(),
        MCP_DB_PORT=str(timescaledb_container.get_exposed_port(5432)),
        MCP_DB_NAME="homelab",
        MCP_DB_USER="test",
        MCP_DB_PASSWORD="test",
        MCP_AUTH_ENABLED="false",
    )
    return Settings()  # type: ignore[call-arg]


@pytest_asyncio.fixture
async def db_pool(settings: Settings) -> AsyncIterator[None]:
    await db.init_pool(settings)
    try:
        yield
    finally:
        await db.close_pool()
