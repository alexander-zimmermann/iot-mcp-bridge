from __future__ import annotations

from pathlib import Path

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

from iot_mcp_bridge.cli.import_knx_catalog import _read_catalog, _to_rows, run


@pytest.fixture
def importer_dsn(timescaledb_container: PostgresContainer) -> str:
    return (
        f"postgresql://test:test@{timescaledb_container.get_container_host_ip()}:"
        f"{timescaledb_container.get_exposed_port(5432)}/homelab"
    )


@pytest.fixture(autouse=True)
def _reset_catalog(importer_dsn: str) -> None:
    """Truncate knx_catalog before each importer test, restore the seed after."""
    seed_rows = [
        ("1/2/0", "Sensors.1F.Bedroom.Temperature", "Bedroom", "Sensors", None, "9.001"),
        ("1/2/1", "Sensors.1F.Bedroom.Humidity", "Bedroom", "Climate", None, "9.007"),
        ("1/2/2", "Lighting.1F.Bedroom.Ceiling", "Bedroom", "Lighting", None, "1.001"),
        ("1/2/3", "Sensors.GF.LivingRoom.Temperature", "LivingRoom", "Sensors", None, "9.001"),
        ("1/2/4", "Sensors.GF.LivingRoom.Humidity", "LivingRoom", "Climate", None, "9.007"),
        ("1/2/100", "Lighting.GF.LivingRoom.Ceiling", "LivingRoom", "Lighting", None, "1.001"),
        ("1/2/200", "Security.GF.LivingRoom.Motion", "LivingRoom", "MotionSensor", None, "1.018"),
        ("1/2/300", "General.Central.Presence", None, None, None, "1.011"),
    ]
    with psycopg.connect(importer_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE knx_catalog")
    yield
    # Restore seed so tests after this module see the original catalog rows.
    with psycopg.connect(importer_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE knx_catalog")
        cur.executemany(
            """
            INSERT INTO knx_catalog (ga, name, room, function, description, dpt)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            seed_rows,
        )


def _write_catalog(tmp: Path, contents: str) -> Path:
    p = tmp / "knx-catalog.yaml"
    p.write_text(contents, encoding="utf-8")
    return p


def test_to_rows_extracts_optional_metadata() -> None:
    catalog = {
        "0/1/40": {
            "name": "Lighting.1F.Bedroom.Ceiling",
            "dpt": "1.001",
            "room": "Bedroom",
            "function": "Lighting",
            "description": "Ceiling light",
        },
        "0/2/0": {"name": "General.Scenes", "dpt": "17.001"},
    }
    rows = _to_rows(catalog)
    assert rows == [
        ("0/1/40", "Lighting.1F.Bedroom.Ceiling", "Bedroom", "Lighting", "Ceiling light", "1.001"),
        ("0/2/0", "General.Scenes", None, None, None, "17.001"),
    ]


def test_read_catalog_rejects_top_level_list(tmp_path: Path) -> None:
    path = _write_catalog(tmp_path, "- not: a-mapping\n")
    with pytest.raises(ValueError, match="expected mapping at top level"):
        _read_catalog(path)


def test_run_inserts_rows(tmp_path: Path, importer_dsn: str) -> None:
    path = _write_catalog(
        tmp_path,
        """
        "1/0/1":
          name: "Lighting.1F.Bedroom.Ceiling"
          dpt: "1.001"
          room: "Bedroom"
          function: "Lighting"
        "1/0/2":
          name: "Sensors.1F.Bedroom.Temperature"
          dpt: "9.001"
          room: "Bedroom"
          function: "Sensors"
        """,
    )
    inserted = run(path, importer_dsn)
    assert inserted == 2

    with psycopg.connect(importer_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT ga, name, room, function FROM knx_catalog ORDER BY ga")
        rows = cur.fetchall()
    assert rows == [
        ("1/0/1", "Lighting.1F.Bedroom.Ceiling", "Bedroom", "Lighting"),
        ("1/0/2", "Sensors.1F.Bedroom.Temperature", "Bedroom", "Sensors"),
    ]


def test_run_is_idempotent(tmp_path: Path, importer_dsn: str) -> None:
    yaml = """
    "1/0/1":
      name: "Lighting.1F.Bedroom.Ceiling"
      dpt: "1.001"
      room: "Bedroom"
      function: "Lighting"
    """
    path = _write_catalog(tmp_path, yaml)
    run(path, importer_dsn)

    # First run set updated_at; record it then re-run.
    with psycopg.connect(importer_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT updated_at FROM knx_catalog WHERE ga = '1/0/1'")
        row = cur.fetchone()
        assert row is not None
        first_updated_at = row[0]

    run(path, importer_dsn)

    with psycopg.connect(importer_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT updated_at FROM knx_catalog WHERE ga = '1/0/1'")
        row = cur.fetchone()
        assert row is not None
        second_updated_at = row[0]

    # Idempotent re-run: WHERE ... IS DISTINCT FROM blocks the UPDATE.
    assert first_updated_at == second_updated_at


def test_run_purges_stale_rows(tmp_path: Path, importer_dsn: str) -> None:
    initial = _write_catalog(
        tmp_path,
        """
        "1/0/1": {name: "A", dpt: "1.001"}
        "1/0/2": {name: "B", dpt: "1.001"}
        """,
    )
    run(initial, importer_dsn)

    pruned = _write_catalog(
        tmp_path,
        """
        "1/0/1": {name: "A", dpt: "1.001"}
        """,
    )
    run(pruned, importer_dsn)

    with psycopg.connect(importer_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT ga FROM knx_catalog ORDER BY ga")
        rows = [r[0] for r in cur.fetchall()]
    assert rows == ["1/0/1"]


def test_run_updates_changed_fields(tmp_path: Path, importer_dsn: str) -> None:
    v1 = _write_catalog(
        tmp_path,
        """
        "1/0/1":
          name: "Old name"
          dpt: "1.001"
          room: "OldRoom"
        """,
    )
    run(v1, importer_dsn)

    v2 = _write_catalog(
        tmp_path,
        """
        "1/0/1":
          name: "New name"
          dpt: "1.001"
          room: "NewRoom"
          function: "Lighting"
        """,
    )
    run(v2, importer_dsn)

    with psycopg.connect(importer_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT name, room, function FROM knx_catalog WHERE ga = '1/0/1'")
        row = cur.fetchone()
    assert row == ("New name", "NewRoom", "Lighting")
