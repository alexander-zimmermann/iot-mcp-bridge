from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Literal

from psycopg import sql

from ..config import Settings
from ..db import connection
from .schema import KIND_CONTINUOUS_AGGREGATE, get_schema, list_data_sources

Aggregation = Literal["avg", "sum", "min", "max", "count"]
_AGG_FUNCS: dict[str, str] = {
    "avg": "AVG",
    "sum": "SUM",
    "min": "MIN",
    "max": "MAX",
    "count": "COUNT",
}

# Postgres interval literal — accept things like '1 hour', '15 minutes', '1 day'.
_INTERVAL_RE = re.compile(
    r"^\s*\d+\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months)\s*$",
    re.IGNORECASE,
)


def _validate_interval(bucket: str) -> str:
    if not _INTERVAL_RE.match(bucket):
        raise ValueError(f"invalid_bucket_interval: {bucket!r}")
    return bucket


def _coarser_or_equal_to_hour(bucket: str) -> bool:
    n_str, unit = bucket.split(maxsplit=1)
    n = int(n_str)
    unit = unit.lower().rstrip("s")
    if unit == "hour":
        return n >= 1
    return unit in {"day", "week", "month"}


async def _resolve_table(
    requested: str,
    bucket: str,
) -> tuple[str, str, str, str]:
    """Pick the table to query (auto-route to *_1h CAGG when possible).

    Returns ``(target_name, target_schema, target_kind, time_column)``.
    """
    sources = await list_data_sources()
    by_name = {s["name"]: s for s in sources}
    if requested not in by_name:
        raise ValueError(f"unknown_table: {requested}")

    chosen = by_name[requested]
    if (
        chosen["kind"] != KIND_CONTINUOUS_AGGREGATE
        and _coarser_or_equal_to_hour(bucket)
        and (cagg := by_name.get(f"{requested}_1h"))
    ):
        chosen = cagg

    if chosen["time_column"] is None:
        raise ValueError(f"no_time_column_detected: {chosen['name']}")
    return chosen["name"], chosen["schema"], chosen["kind"], chosen["time_column"]


async def _validated_columns(table: str, columns: list[str]) -> list[str]:
    schema = await get_schema(table)
    known = {c["name"] for c in schema["columns"]}
    unknown = [c for c in columns if c not in known]
    if unknown:
        raise ValueError(f"unknown_columns: {unknown}")
    return columns


def _validate_filters(
    filters: dict[str, Any] | None,
    valid_columns: set[str],
) -> dict[str, Any]:
    if not filters:
        return {}
    bad = [k for k in filters if k not in valid_columns]
    if bad:
        raise ValueError(f"unknown_filter_columns: {bad}")
    return filters


async def query_timeseries(
    table: str,
    columns: list[str],
    from_ts: str | datetime,
    to_ts: str | datetime,
    aggregation: Aggregation = "avg",
    bucket: str = "1 hour",
    filters: dict[str, Any] | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Bucketed aggregate query against a hypertable or continuous aggregate.

    - ``aggregation`` is one of ``avg|sum|min|max|count``.
    - When ``bucket`` is one hour or coarser AND a ``<table>_1h`` continuous
      aggregate exists, the query is transparently routed to the CAGG.
    - The result is capped at ``MCP_QUERY_ROW_LIMIT`` rows; over-shoot returns
      an error suggesting a coarser bucket.
    - ``filters`` is a flat ``{column: value}`` dict translated to ``=`` predicates.
    """
    if aggregation not in _AGG_FUNCS:
        raise ValueError(f"invalid_aggregation: {aggregation!r}")
    bucket = _validate_interval(bucket)

    target, target_schema, target_kind, time_col = await _resolve_table(table, bucket)
    cols = await _validated_columns(target, columns)

    schema = await get_schema(target)
    valid_cols = {c["name"] for c in schema["columns"]}
    flt = _validate_filters(filters, valid_cols)

    select_parts: list[sql.Composable] = [
        sql.SQL("time_bucket(%s, {col}) AS bucket").format(col=sql.Identifier(time_col))
    ]
    for c in cols:
        select_parts.append(
            sql.SQL("{fn}({col}) AS {alias}").format(
                fn=sql.SQL(_AGG_FUNCS[aggregation]),
                col=sql.Identifier(c),
                alias=sql.Identifier(f"{c}_{aggregation}"),
            )
        )

    where_parts: list[sql.Composable] = [
        sql.SQL("{col} BETWEEN %s AND %s").format(col=sql.Identifier(time_col))
    ]
    params: list[Any] = [bucket, from_ts, to_ts]
    for k, v in flt.items():
        where_parts.append(sql.SQL("{col} = %s").format(col=sql.Identifier(k)))
        params.append(v)

    if settings is None:
        raise ValueError("settings is required")
    row_limit = settings.query_row_limit
    stmt = sql.SQL(
        "SELECT {selects} FROM {tbl} WHERE {where} GROUP BY bucket "
        "ORDER BY bucket LIMIT %s"
    ).format(
        selects=sql.SQL(", ").join(select_parts),
        tbl=sql.Identifier(target_schema, target),
        where=sql.SQL(" AND ").join(where_parts),
    )
    params.append(row_limit + 1)

    async with connection() as conn:
        rows = await (await conn.execute(stmt, params)).fetchall()

    if len(rows) > row_limit:
        raise ValueError(
            f"row_limit_exceeded: result would exceed {row_limit} rows; "
            "widen the bucket or shorten the time range"
        )

    return {
        "table_requested": table,
        "table_used": target,
        "kind_used": target_kind,
        "bucket": bucket,
        "aggregation": aggregation,
        "row_count": len(rows),
        "rows": [
            {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}
            for row in rows
        ],
    }
