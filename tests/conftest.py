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
    container.start()
    try:
        conn_kwargs = {
            "host": container.get_container_host_ip(),
            "port": int(container.get_exposed_port(5432)),
            "user": "test",
            "password": "test",
            "dbname": "homelab",
            "autocommit": True,
        }
        # 1) Extension + tables + seed (regular DDL/DML, autocommit-safe).
        conn = psycopg.connect(**conn_kwargs)
        try:
            conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            _seed(conn)
        finally:
            conn.close()

        # 2) Continuous aggregate setup. `CALL refresh_continuous_aggregate()`
        #    must run outside any transaction block — open a fresh connection
        #    and issue each statement separately so psycopg cannot wrap them.
        conn = psycopg.connect(**conn_kwargs)
        try:
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
                WITH NO DATA
                """
            )
            conn.execute(
                """
                SELECT add_continuous_aggregate_policy(
                    'knx_1h',
                    start_offset => INTERVAL '7 days',
                    end_offset   => INTERVAL '5 minutes',
                    schedule_interval => INTERVAL '1 hour'
                )
                """
            )
            conn.execute("CALL refresh_continuous_aggregate('knx_1h', NULL, NULL)")
        finally:
            conn.close()

        yield container
    finally:
        container.stop()


def _seed(conn: psycopg.Connection) -> None:
    """Set up two hypertables and seed a few rows. Caller owns the connection."""
    conn.execute(
        """
        CREATE TABLE knx (
            time TIMESTAMPTZ NOT NULL,
            ga   TEXT NOT NULL,
            dpt  TEXT,
            value DOUBLE PRECISION,
            raw  JSONB
        )
        """
    )
    conn.execute("SELECT create_hypertable('knx', by_range('time'))")
    conn.execute(
        """
        CREATE TABLE ems_esp (
            time TIMESTAMPTZ NOT NULL,
            topic TEXT NOT NULL,
            raw  JSONB NOT NULL
        )
        """
    )
    conn.execute("SELECT create_hypertable('ems_esp', by_range('time'))")
    conn.execute(
        """
        INSERT INTO knx
        SELECT
            NOW() - (i || ' minutes')::interval,
            '1/2/' || (i % 5),
            '9.001',
            20 + (i % 10),
            jsonb_build_object('value', 20 + (i % 10), 'unit', 'celsius')
        FROM generate_series(0, 200) AS i
        """
    )
    conn.execute(
        """
        INSERT INTO ems_esp
        SELECT
            NOW() - (i || ' minutes')::interval,
            'boiler_data',
            jsonb_build_object(
                'flow_temp', 35 + (i % 20),
                'return_temp', 30 + (i % 15),
                'burner_power', CASE WHEN i % 3 = 0 THEN 50 ELSE 0 END
            )
        FROM generate_series(0, 200) AS i
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
