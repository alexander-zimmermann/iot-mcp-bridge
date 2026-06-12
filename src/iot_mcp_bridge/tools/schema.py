"""Schema discovery tools — data-source catalog and per-table description."""

from __future__ import annotations

from typing import Any

from cachetools import TTLCache
from psycopg import sql

from .. import metrics as metrics_module
from ..db import connection

KIND_HYPERTABLE = "hypertable"
KIND_CONTINUOUS_AGGREGATE = "continuous_aggregate"

# Every tool call funnels through list_data_sources() for validation and CAGG
# routing, and the underlying catalog walk (2 + 2N queries incl. a MIN/MAX scan
# per source) dwarfs the actual tool query. Hypertables and CAGGs change rarely,
# so a short single-entry TTL cache removes the repeat cost; new tables show up
# after at most one TTL period.
_SOURCES_CACHE_TTL_SECONDS = 60
# Alias pins the key/value types for both mypy and Pyright — the cachetools
# stubs cannot infer them from an otherwise-empty constructor.
_SourcesCache = TTLCache[str, list[dict[str, Any]]]
_sources_cache: _SourcesCache = _SourcesCache(maxsize=1, ttl=_SOURCES_CACHE_TTL_SECONDS)


def invalidate_cache() -> None:
    """Drop the cached data-source catalog (used by tests for isolation)."""
    _sources_cache.clear()


_LIST_HYPERTABLES_SQL = """
SELECT
    h.hypertable_schema AS schema,
    h.hypertable_name   AS name,
    obj_description(format('%I.%I', h.hypertable_schema, h.hypertable_name)::regclass, 'pg_class')
        AS description
FROM timescaledb_information.hypertables h
ORDER BY h.hypertable_name
"""

_LIST_CAGGS_SQL = """
SELECT
    c.view_schema AS schema,
    c.view_name   AS name,
    obj_description(format('%I.%I', c.view_schema, c.view_name)::regclass, 'pg_class')
        AS description
FROM timescaledb_information.continuous_aggregates c
ORDER BY c.view_name
"""

_TIME_RANGE_SQL = sql.SQL("SELECT MIN({col}) AS min_ts, MAX({col}) AS max_ts FROM {tbl}")

_COLUMNS_SQL = """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = %s AND table_name = %s
ORDER BY ordinal_position
"""

_JSONB_KEYS_SQL = sql.SQL(
    """
    SELECT key, COUNT(*)::bigint AS occurrences
    FROM (
        SELECT jsonb_object_keys({col}) AS key
        FROM {tbl}
        ORDER BY {time_col} DESC
        LIMIT %s
    ) sub
    GROUP BY key
    ORDER BY occurrences DESC
    LIMIT %s
    """
)


async def _detect_time_column(schema: str, name: str) -> str | None:
    """Find the most likely time column on a hypertable / CAGG.

    Hypertables use `time` by convention in this project; CAGGs typically use
    `bucket`. We pick the first ``timestamp with time zone`` column.
    """
    async with connection() as conn:
        cur = await conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
              AND data_type = 'timestamp with time zone'
            ORDER BY
                CASE column_name
                    WHEN 'time' THEN 0
                    WHEN 'bucket' THEN 1
                    ELSE 2
                END,
                ordinal_position
            LIMIT 1
            """,
            (schema, name),
        )
        row = await cur.fetchone()
        return row["column_name"] if row else None


async def list_data_sources() -> list[dict[str, Any]]:
    """List all hypertables and continuous aggregates, with their time range.

    Each entry contains: ``schema``, ``name``, ``kind``, ``description``,
    ``time_column``, ``time_range`` (``min``/``max``).
    """
    cached = _sources_cache.get("sources")
    if cached is not None:
        return cached

    m = metrics_module.get()
    m.db_queries.labels(tool="list_data_sources", table_used="_metadata").inc()
    with m.db_query_duration.labels(tool="list_data_sources").time():
        async with connection() as conn:
            hyper = await (await conn.execute(_LIST_HYPERTABLES_SQL)).fetchall()
            caggs = await (await conn.execute(_LIST_CAGGS_SQL)).fetchall()

        rows: list[dict[str, Any]] = []
        for r in hyper:
            rows.append({**r, "kind": KIND_HYPERTABLE})
        for r in caggs:
            rows.append({**r, "kind": KIND_CONTINUOUS_AGGREGATE})

        out: list[dict[str, Any]] = []
        for r in rows:
            time_col = await _detect_time_column(r["schema"], r["name"])
            time_range: dict[str, Any] = {"min": None, "max": None}
            if time_col:
                stmt = _TIME_RANGE_SQL.format(
                    col=sql.Identifier(time_col),
                    tbl=sql.Identifier(r["schema"], r["name"]),
                )
                async with connection() as conn:
                    tr = await (await conn.execute(stmt)).fetchone()
                    if tr:
                        time_range = {
                            "min": tr["min_ts"].isoformat() if tr["min_ts"] else None,
                            "max": tr["max_ts"].isoformat() if tr["max_ts"] else None,
                        }
            out.append(
                {
                    "name": r["name"],
                    "schema": r["schema"],
                    "kind": r["kind"],
                    "description": r["description"],
                    "time_column": time_col,
                    "time_range": time_range,
                }
            )
        _sources_cache["sources"] = out
        return out


async def get_schema(table: str, jsonb_sample_size: int = 1000) -> dict[str, Any]:
    """Describe a table: columns, types, time column, JSONB key sample.

    The ``table`` name is validated against ``list_data_sources()``. Unknown
    tables raise ``ValueError``.
    """
    sources = await list_data_sources()
    match = next((s for s in sources if s["name"] == table), None)
    if match is None:
        raise ValueError(f"unknown_table: {table}")

    schema_name = match["schema"]
    m = metrics_module.get()
    m.db_queries.labels(tool="get_schema", table_used=table).inc()
    with m.db_query_duration.labels(tool="get_schema").time():
        async with connection() as conn:
            columns = await (await conn.execute(_COLUMNS_SQL, (schema_name, table))).fetchall()

        jsonb_columns = [c["column_name"] for c in columns if c["data_type"] == "jsonb"]
        jsonb_samples: dict[str, list[dict[str, Any]]] = {}
        time_col = match["time_column"]
        if time_col:
            for col in jsonb_columns:
                stmt = _JSONB_KEYS_SQL.format(
                    col=sql.Identifier(col),
                    tbl=sql.Identifier(schema_name, table),
                    time_col=sql.Identifier(time_col),
                )
                async with connection() as conn:
                    rows = await (await conn.execute(stmt, (jsonb_sample_size, 30))).fetchall()
                jsonb_samples[col] = [
                    {"key": r["key"], "occurrences": r["occurrences"]} for r in rows
                ]

    hint = None
    if table == "knx":
        hint = (
            "Prefer querying ga_catalog_view (knx joined with ga_catalog) — it "
            "exposes ga_name, room, function, description per row, so queries can "
            "filter or group by room/function without round-tripping the catalog."
        )

    return {
        "name": table,
        "schema": schema_name,
        "kind": match["kind"],
        "time_column": time_col,
        "columns": [
            {
                "name": c["column_name"],
                "type": c["data_type"],
                "nullable": c["is_nullable"] == "YES",
            }
            for c in columns
        ],
        "jsonb_top_keys": jsonb_samples,
        "hint": hint,
    }
