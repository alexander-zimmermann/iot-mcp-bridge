"""Import the KNX catalog YAML into the Postgres ``knx_catalog`` table.

One-shot CLI invoked from a K8s Job. Reads MCP_DB_* env vars (same as the
MCP server) plus a ``--catalog-path`` argument. Idempotent:

* INSERT ... ON CONFLICT (ga) DO UPDATE -- updates only when something
  actually differs (no spurious updated_at churn)
* DELETE rows whose GA is no longer in the YAML (tombstone purge)

Connects with admin credentials -- the read-only role used by the MCP
server cannot write to ``knx_catalog``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, cast

import psycopg
import yaml

from ..config import load_settings

logger = logging.getLogger(__name__)


_UPSERT_SQL = """
INSERT INTO knx_catalog (ga, name, room, function, description, dpt, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, now())
ON CONFLICT (ga) DO UPDATE SET
    name        = EXCLUDED.name,
    room        = EXCLUDED.room,
    function    = EXCLUDED.function,
    description = EXCLUDED.description,
    dpt         = EXCLUDED.dpt,
    updated_at  = now()
WHERE
    knx_catalog.name        IS DISTINCT FROM EXCLUDED.name OR
    knx_catalog.room        IS DISTINCT FROM EXCLUDED.room OR
    knx_catalog.function    IS DISTINCT FROM EXCLUDED.function OR
    knx_catalog.description IS DISTINCT FROM EXCLUDED.description OR
    knx_catalog.dpt         IS DISTINCT FROM EXCLUDED.dpt
"""


def _read_catalog(path: Path) -> dict[str, Any]:
    raw: object = yaml.safe_load(path.read_text(encoding="utf-8"))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: expected mapping at top level, got {type(raw).__name__}")
    return cast("dict[str, Any]", raw)


def _to_rows(catalog: dict[str, Any]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for ga, entry in catalog.items():
        if not isinstance(entry, dict):
            raise ValueError(f"entry for {ga!r} must be a mapping")
        e = cast("dict[str, Any]", entry)
        name = e.get("name")
        dpt = e.get("dpt")
        if not isinstance(name, str) or not isinstance(dpt, str):
            raise ValueError(f"entry for {ga!r} requires string 'name' and 'dpt'")
        rows.append(
            (
                ga,
                name,
                e.get("room"),
                e.get("function"),
                e.get("description"),
                dpt,
            )
        )
    return rows


def run(catalog_path: Path, dsn: str) -> int:
    catalog = _read_catalog(catalog_path)
    rows = _to_rows(catalog)
    if not rows:
        logger.warning("catalog is empty: %s", catalog_path)
        return 0

    keys = list(catalog.keys())
    with psycopg.connect(dsn, autocommit=False) as conn, conn.cursor() as cur:
        cur.executemany(_UPSERT_SQL, rows)
        cur.execute("DELETE FROM knx_catalog WHERE ga != ALL(%s)", (keys,))
        deleted = cur.rowcount
        conn.commit()

    logger.info(
        "imported %d entries from %s; deleted %d stale row(s)",
        len(rows),
        catalog_path,
        max(deleted, 0),
    )
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Import KNX catalog into knx_catalog")
    parser.add_argument(
        "--catalog-path",
        type=Path,
        required=True,
        help="Path to the knx-catalog.yaml file",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    settings = load_settings()
    run(args.catalog_path, settings.db_dsn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
