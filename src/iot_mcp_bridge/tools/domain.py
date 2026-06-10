"""Domain tools — high-level queries that hide table layout / joins."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from psycopg import sql

from .. import metrics as metrics_module
from ..config import Settings
from ..db import connection
from .timeseries import _coarser_or_equal_to_hour, _resolve_table, _validate_interval


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}


def _check_row_limit(rows: list[Any], row_limit: int, hint: str) -> None:
    if len(rows) > row_limit:
        raise ValueError(f"row_limit_exceeded: result would exceed {row_limit} rows; {hint}")


# =====================================================================
# query_energy_flow
# =====================================================================

_ENERGY_FLOW_SQL = sql.SQL(
    """
    SELECT
        coalesce(pf.bucket, m.bucket)                AS bucket,
        pf.pv_production_avg                          AS pv_production_avg,
        pf.grid_power_avg                             AS grid_power_avg,
        pf.consumer_total_avg                         AS consumer_total_avg,
        pf.battery_net_avg                            AS battery_net_avg,
        m.evcharger_power_avg                         AS evcharger_power_avg
    FROM (
        SELECT bucket, avg(pv_production_avg)   AS pv_production_avg,
                       avg(grid_power_avg)      AS grid_power_avg,
                       avg(consumer_total_avg)  AS consumer_total_avg,
                       avg(battery_net_avg)     AS battery_net_avg
        FROM solaredge_powerflow_1h
        WHERE bucket BETWEEN %s AND %s
        GROUP BY bucket
    ) pf
    FULL OUTER JOIN (
        SELECT bucket, sum(power_total_avg) AS evcharger_power_avg
        FROM warp_meter_1h
        WHERE bucket BETWEEN %s AND %s
        GROUP BY bucket
    ) m USING (bucket)
    ORDER BY bucket
    LIMIT %s
    """
)


async def query_energy_flow(
    from_ts: str | datetime,
    to_ts: str | datetime,
    settings: Settings,
    bucket: str = "1 hour",
) -> dict[str, Any]:
    """Joined energy flow per bucket: PV / grid / consumer / battery / wallbox.

    Pulls from the hourly continuous aggregates ``solaredge_powerflow_1h`` and
    ``warp_meter_1h`` (the only source of wallbox power) and FULL OUTER JOINs
    them on ``bucket``. Buckets coarser than the underlying CAGG (``1 hour``)
    are aggregated with ``avg()``.
    """
    bucket = _validate_interval(bucket)
    if not _coarser_or_equal_to_hour(bucket):
        raise ValueError(f"invalid_bucket: {bucket!r} — energy_flow uses 1h CAGGs, need >=1 hour")

    row_limit = settings.query_row_limit
    m = metrics_module.get()
    m.db_queries.labels(tool="query_energy_flow", table_used="solaredge_powerflow_1h").inc()
    with m.db_query_duration.labels(tool="query_energy_flow").time():
        async with connection() as conn:
            cur = await conn.execute(
                _ENERGY_FLOW_SQL,
                (from_ts, to_ts, from_ts, to_ts, row_limit + 1),
            )
            rows = await cur.fetchall()

    _check_row_limit(rows, row_limit, "widen the bucket or shorten the time range")
    return {
        "bucket": bucket,
        "from_ts": str(from_ts),
        "to_ts": str(to_ts),
        "row_count": len(rows),
        "rows": [_serialize_row(r) for r in rows],
    }


# =====================================================================
# query_heating_cycles
# =====================================================================

_HEATING_CYCLES_SQL = sql.SQL(
    """
    WITH samples AS (
        SELECT time,
               curburnpow,
               (curburnpow > 0)::int AS on_state,
               LAG((curburnpow > 0)::int) OVER (ORDER BY time) AS prev_state
        FROM ems_esp
        WHERE topic = 'boiler_data'
          AND time BETWEEN %s AND %s
    ),
    transitions AS (
        SELECT *,
               CASE WHEN on_state IS DISTINCT FROM prev_state THEN 1 ELSE 0 END AS is_change
        FROM samples
    ),
    cycles AS (
        SELECT *, SUM(is_change) OVER (ORDER BY time) AS cycle_id
        FROM transitions
    ),
    aggregated AS (
        SELECT
            cycle_id,
            MIN(time)                               AS start_ts,
            MAX(time)                               AS end_ts,
            EXTRACT(EPOCH FROM (MAX(time) - MIN(time)))::float AS duration_seconds,
            MAX(curburnpow)::float                  AS peak_power_kw,
            AVG(curburnpow)::float                  AS avg_power_kw
        FROM cycles
        WHERE on_state = 1
        GROUP BY cycle_id
    )
    SELECT start_ts, end_ts, duration_seconds, peak_power_kw, avg_power_kw
    FROM aggregated
    WHERE duration_seconds >= %s
    ORDER BY start_ts
    LIMIT %s
    """
)


async def query_heating_cycles(
    from_ts: str | datetime,
    to_ts: str | datetime,
    settings: Settings,
    min_duration_seconds: int = 60,
) -> dict[str, Any]:
    """Detect ON/OFF burner cycles from raw ``ems_esp`` boiler telemetry.

    A cycle is a contiguous run with ``curburnpow > 0``; transitions are
    detected via ``LAG`` window. Returns one row per cycle with start, end,
    duration, peak and average burner power. Cycles shorter than
    ``min_duration_seconds`` are filtered (defaults to 60 s).
    """
    if min_duration_seconds < 0:
        raise ValueError(f"invalid_min_duration_seconds: {min_duration_seconds}")

    row_limit = settings.query_row_limit
    m = metrics_module.get()
    m.db_queries.labels(tool="query_heating_cycles", table_used="ems_esp").inc()
    with m.db_query_duration.labels(tool="query_heating_cycles").time():
        async with connection() as conn:
            cur = await conn.execute(
                _HEATING_CYCLES_SQL,
                (from_ts, to_ts, min_duration_seconds, row_limit + 1),
            )
            rows = await cur.fetchall()

    _check_row_limit(rows, row_limit, "shorten the time range or raise min_duration_seconds")

    serialized = [_serialize_row(r) for r in rows]
    total_runtime = sum(float(r["duration_seconds"]) for r in serialized)
    return {
        "from_ts": str(from_ts),
        "to_ts": str(to_ts),
        "min_duration_seconds": min_duration_seconds,
        "cycle_count": len(serialized),
        "total_runtime_seconds": total_runtime,
        "cycles": serialized,
    }


# =====================================================================
# query_room_climate
# =====================================================================

_DISTINCT_ROOMS_SQL = "SELECT DISTINCT room FROM ga_catalog WHERE room IS NOT NULL ORDER BY room"
_DISTINCT_FUNCTIONS_SQL = (
    "SELECT DISTINCT function FROM ga_catalog WHERE function IS NOT NULL ORDER BY function"
)


async def _known_rooms() -> list[str]:
    async with connection() as conn:
        rows = await (await conn.execute(_DISTINCT_ROOMS_SQL)).fetchall()
    return [r["room"] for r in rows]


async def _known_functions() -> list[str]:
    async with connection() as conn:
        rows = await (await conn.execute(_DISTINCT_FUNCTIONS_SQL)).fetchall()
    return [r["function"] for r in rows]


async def query_room_climate(
    room: str,
    from_ts: str | datetime,
    to_ts: str | datetime,
    settings: Settings,
    bucket: str = "1 hour",
    functions: list[str] | None = None,
) -> dict[str, Any]:
    """Bucketed average reading per GA name within a room.

    ``room`` is validated against the distinct values in ``ga_catalog`` so
    unknown rooms produce a precise error the LLM can self-correct against.

    Optional ``functions`` narrows the result to GAs whose ETS function name
    is in the given list (e.g. climate-style names the LLM picks from
    ``query_knx_events`` / ``get_schema``). When omitted, all GAs in the room
    are aggregated — the project's function vocabulary is not assumed.
    """
    bucket = _validate_interval(bucket)

    valid_rooms = await _known_rooms()
    if room not in valid_rooms:
        raise ValueError(f"unknown_room: {room!r}; valid: {valid_rooms}")

    where_parts: list[sql.Composable] = [
        sql.SQL("room = %s"),
        sql.SQL("time BETWEEN %s AND %s"),
    ]
    params: list[Any] = [bucket, room, from_ts, to_ts]
    if functions:
        where_parts.append(sql.SQL("function = ANY(%s)"))
        params.append(list(functions))
    params.append(settings.query_row_limit + 1)

    stmt = sql.SQL(
        """
        SELECT
            time_bucket(%s, time) AS bucket,
            ga_name,
            AVG(value)::float     AS value_avg
        FROM ga_catalog_view
        WHERE {where}
        GROUP BY bucket, ga_name
        ORDER BY bucket, ga_name
        LIMIT %s
        """
    ).format(where=sql.SQL(" AND ").join(where_parts))

    row_limit = settings.query_row_limit
    m = metrics_module.get()
    m.db_queries.labels(tool="query_room_climate", table_used="ga_catalog_view").inc()
    with m.db_query_duration.labels(tool="query_room_climate").time():
        async with connection() as conn:
            cur = await conn.execute(stmt, params)
            rows = await cur.fetchall()

    _check_row_limit(rows, row_limit, "widen the bucket or shorten the time range")
    return {
        "room": room,
        "bucket": bucket,
        "from_ts": str(from_ts),
        "to_ts": str(to_ts),
        "functions": functions,
        "row_count": len(rows),
        "rows": [_serialize_row(r) for r in rows],
    }


# =====================================================================
# query_knx_events
# =====================================================================


async def query_knx_events(
    from_ts: str | datetime,
    to_ts: str | datetime,
    settings: Settings,
    room: str | None = None,
    ga: str | None = None,
    name: str | None = None,
    functions: list[str] | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """Raw event log against ``ga_catalog_view``.

    Predicates (all optional, AND-combined):

    * ``room``      — exact match against ``ga_catalog.room``
    * ``ga``        — exact GA match (``"4/2/161"``)
    * ``name``      — substring match against ``ga_name`` via ``ILIKE``;
                      pass partial words like ``"Beleuchtung"``
    * ``functions`` — list of ETS function names to include (e.g.
                      ``["Beleuchtung", "Schalten"]``); validated against
                      the catalog so invalid names produce a precise error

    Default ``limit`` is 200 (typical "what happened recently" window).
    Effective cap is ``min(limit, settings.query_row_limit)``.
    """
    if limit <= 0:
        raise ValueError(f"invalid_limit: {limit}")
    effective_limit = min(limit, settings.query_row_limit)

    if functions:
        valid_functions = await _known_functions()
        unknown = sorted(set(functions) - set(valid_functions))
        if unknown:
            raise ValueError(f"unknown_functions: {unknown}; valid: {valid_functions}")

    where_parts: list[sql.Composable] = [sql.SQL("time BETWEEN %s AND %s")]
    params: list[Any] = [from_ts, to_ts]
    if room is not None:
        where_parts.append(sql.SQL("room = %s"))
        params.append(room)
    if ga is not None:
        where_parts.append(sql.SQL("ga = %s"))
        params.append(ga)
    if name is not None:
        where_parts.append(sql.SQL("ga_name ILIKE %s"))
        params.append(f"%{name}%")
    if functions:
        where_parts.append(sql.SQL("function = ANY(%s)"))
        params.append(list(functions))
    params.append(effective_limit + 1)

    stmt = sql.SQL(
        """
        SELECT time, ga, ga_name, room, function, dpt, value
        FROM ga_catalog_view
        WHERE {where}
        ORDER BY time DESC
        LIMIT %s
        """
    ).format(where=sql.SQL(" AND ").join(where_parts))

    m = metrics_module.get()
    m.db_queries.labels(tool="query_knx_events", table_used="ga_catalog_view").inc()
    with m.db_query_duration.labels(tool="query_knx_events").time():
        async with connection() as conn:
            rows = await (await conn.execute(stmt, params)).fetchall()

    _check_row_limit(rows, effective_limit, "narrow by room/ga/name or shorten time range")
    return {
        "from_ts": str(from_ts),
        "to_ts": str(to_ts),
        "filters": {"room": room, "ga": ga, "name": name, "functions": functions},
        "limit": effective_limit,
        "row_count": len(rows),
        "rows": [_serialize_row(r) for r in rows],
    }


# =====================================================================
# query_unifi_events
# =====================================================================


async def query_unifi_events(
    from_ts: str | datetime,
    to_ts: str | datetime,
    settings: Settings,
    camera: str | None = None,
    detection_type: str | None = None,
    event_type: str | None = None,
    min_score: int | None = None,
    event_id: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """Recent UniFi Protect Alarm Manager events for security review.

    Each row is one trigger published on ``unifi.events.<camera>.<detection_type>``
    by node-RED and ingested into ``unifi_events`` by redpanda-connect.

    Predicates (all optional, AND-combined):

    * ``camera``         — exact camera name (e.g. ``"fassade"``, ``"eingang"``,
                           ``"terrasse_wohnzimmer"``, ``"terrasse_esszimmer"``)
    * ``detection_type`` — trigger ``key`` (e.g. ``"person"``, ``"motion"``,
                           ``"face_known"``, ``"vehicle"``,
                           ``"license_plate_known"``, ``"audio_alarm_siren"``)
    * ``event_type``     — UniFi-internal source type
                           (``"smartDetectZone"`` / ``"smartDetectLine"`` /
                           ``"motion"``)
    * ``min_score``      — confidence floor 0..100; UniFi smart-detect score
                           lives in ``sourceEvent.score``
    * ``event_id``       — exact UUID; use to look up details for one alarm

    Default ``limit`` is 200; effective cap is
    ``min(limit, settings.query_row_limit)``.
    """
    if limit <= 0:
        raise ValueError(f"invalid_limit: {limit}")
    if min_score is not None and not 0 <= min_score <= 100:
        raise ValueError(f"invalid_min_score: {min_score}; must be 0..100")
    effective_limit = min(limit, settings.query_row_limit)

    where_parts: list[sql.Composable] = [sql.SQL("time BETWEEN %s AND %s")]
    params: list[Any] = [from_ts, to_ts]
    if camera is not None:
        where_parts.append(sql.SQL("camera = %s"))
        params.append(camera)
    if detection_type is not None:
        where_parts.append(sql.SQL("detection_type = %s"))
        params.append(detection_type)
    if event_type is not None:
        where_parts.append(sql.SQL("event_type = %s"))
        params.append(event_type)
    if min_score is not None:
        where_parts.append(sql.SQL("score >= %s"))
        params.append(min_score)
    if event_id is not None:
        where_parts.append(sql.SQL("event_id = %s"))
        params.append(event_id)
    params.append(effective_limit + 1)

    stmt = sql.SQL(
        """
        SELECT time, camera, detection_type, score, event_type, value, event_id, event_link
        FROM unifi_events
        WHERE {where}
        ORDER BY time DESC
        LIMIT %s
        """
    ).format(where=sql.SQL(" AND ").join(where_parts))

    m = metrics_module.get()
    m.db_queries.labels(tool="query_unifi_events", table_used="unifi_events").inc()
    with m.db_query_duration.labels(tool="query_unifi_events").time():
        async with connection() as conn:
            rows = await (await conn.execute(stmt, params)).fetchall()

    _check_row_limit(rows, effective_limit, "narrow by camera/detection_type or shorten time range")
    return {
        "from_ts": str(from_ts),
        "to_ts": str(to_ts),
        "filters": {
            "camera": camera,
            "detection_type": detection_type,
            "event_type": event_type,
            "min_score": min_score,
            "event_id": event_id,
        },
        "limit": effective_limit,
        "row_count": len(rows),
        "rows": [_serialize_row(r) for r in rows],
    }


# =====================================================================
# correlate_events
# =====================================================================

_CORRELATE_SQL_TEMPLATE = sql.SQL(
    """
    WITH a AS (
        SELECT time_bucket(%s, {time_a}) AS bts, AVG({col_a})::float AS v
        FROM {tbl_a}
        WHERE {time_a} BETWEEN %s AND %s
        GROUP BY bts
    ),
    b AS (
        SELECT time_bucket(%s, {time_b}) AS bts, AVG({col_b})::float AS v
        FROM {tbl_b}
        WHERE {time_b} BETWEEN %s AND %s
        GROUP BY bts
    ),
    lags AS (
        SELECT generate_series(-%s::int, %s::int)::int AS lag
    ),
    joined AS (
        SELECT lags.lag,
               corr(a.v, b.v)::float AS c,
               COUNT(*)::int          AS n
        FROM lags
        CROSS JOIN a
        INNER JOIN b ON b.bts = a.bts + (lags.lag * (%s)::interval)
        GROUP BY lags.lag
        HAVING COUNT(*) >= 3
    )
    SELECT lag, c, n FROM joined ORDER BY abs(c) DESC NULLS LAST LIMIT %s
    """
)


async def correlate_events(
    source_a: dict[str, Any],
    source_b: dict[str, Any],
    from_ts: str | datetime,
    to_ts: str | datetime,
    settings: Settings,
    window: str = "15 minutes",
    bucket: str = "1 minute",
) -> dict[str, Any]:
    """Lagged Pearson correlation between two time-series streams.

    Each ``source`` is ``{"table": str, "column": str}``. Both streams are
    bucketed to ``bucket`` width, then for every lag in
    ``[-N, +N]`` (where ``N = window / bucket``) the correlation is computed
    over matched buckets. The top 10 lags by ``|corr|`` are returned.
    """
    bucket = _validate_interval(bucket)
    window = _validate_interval(window)

    n_lags = _interval_ratio(window, bucket)
    if n_lags <= 0:
        raise ValueError(f"window must be larger than bucket; got window={window}, bucket={bucket}")

    table_a = source_a["table"]
    column_a = source_a["column"]
    table_b = source_b["table"]
    column_b = source_b["column"]

    target_a, schema_a, _, time_col_a = await _resolve_table(table_a, bucket)
    target_b, schema_b, _, time_col_b = await _resolve_table(table_b, bucket)

    stmt = _CORRELATE_SQL_TEMPLATE.format(
        time_a=sql.Identifier(time_col_a),
        col_a=sql.Identifier(column_a),
        tbl_a=sql.Identifier(schema_a, target_a),
        time_b=sql.Identifier(time_col_b),
        col_b=sql.Identifier(column_b),
        tbl_b=sql.Identifier(schema_b, target_b),
    )
    top_n = min(10, settings.query_row_limit)
    params = (
        bucket,
        from_ts,
        to_ts,
        bucket,
        from_ts,
        to_ts,
        n_lags,
        n_lags,
        bucket,
        top_n,
    )

    m = metrics_module.get()
    m.db_queries.labels(tool="correlate_events", table_used=f"{target_a}+{target_b}").inc()
    with m.db_query_duration.labels(tool="correlate_events").time():
        async with connection() as conn:
            rows = await (await conn.execute(stmt, params)).fetchall()

    top: list[dict[str, Any]] = [
        {
            "lag_buckets": int(r["lag"]),
            "corr": float(r["c"]) if r["c"] is not None else None,
            "samples": int(r["n"]),
        }
        for r in rows
    ]
    best = top[0] if top else None
    return {
        "source_a": {"table": target_a, "column": column_a, "time_column": time_col_a},
        "source_b": {"table": target_b, "column": column_b, "time_column": time_col_b},
        "bucket": bucket,
        "window": window,
        "n_lags": n_lags,
        "best": best,
        "top": top,
    }


def _interval_ratio(window: str, bucket: str) -> int:
    """Compute how many ``bucket``-units fit into ``window`` (integer divide)."""
    units_per_second = {
        "second": 1,
        "minute": 60,
        "hour": 3600,
        "day": 86400,
        "week": 604800,
        "month": 2_592_000,
    }

    def _to_seconds(label: str) -> int:
        n_str, unit = label.split(maxsplit=1)
        unit = unit.lower().rstrip("s")
        return int(n_str) * units_per_second[unit]

    return _to_seconds(window) // _to_seconds(bucket)
