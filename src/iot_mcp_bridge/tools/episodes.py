"""The verdict loop: read the episodes the detection chain holds, and attach
a binary verdict to one of them.

The mails Basalte sends are the prompt to review, not the archive — the
engine already knows every situation it caused, so the review happens
against this list. A verdict says only whether the situation was real or
nonsense, and it hangs on the individual episode rather than the fault, so
it stays possible to see *when* a fault is wrong: only at night, only in
summer, only while the laundry runs.

Nothing acts on a verdict. They are counted per fault on the dashboard and
thresholds are moved by a person with those numbers in view — which is what
turns tuning from an opinion into a measurement. ``set_episode_verdict`` is
the server's only write, and it goes through the separate write pool.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from psycopg import sql

from .. import metrics as metrics_module
from ..config import Settings
from ..db import connection, write_connection

# Binary by design — nobody sustains a richer scale, and for the only
# question that matters (how often does this fault get it wrong) it is enough.
VERDICTS = ("real", "nonsense")

EpisodeState = Literal["all", "open", "ended"]

_MAX_WINDOW_DAYS = 365 * 5

# The subject carries the group address it was observed on; the catalog turns
# that into the name and room a person recognises. Falls back to the bracketed
# label, then to the subject itself, for subjects that carry no address.
_SUBJECT_SQL = sql.SQL(
    """
    LEFT JOIN ga_catalog c ON c.ga = substring(e.subject from '[0-9]+/[0-9]+/[0-9]+')
    """
)


def _serialize(row: dict[str, Any]) -> dict[str, Any]:
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}


async def list_episodes(
    *,
    settings: Settings,
    state: EpisodeState = "all",
    fault: str | None = None,
    days: int = 7,
    only_unrated: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    """Episodes overlapping the last ``days``, newest first, each with the
    verdict it already carries.

    The window is an overlap, not a start filter: an episode that began
    weeks ago and is still open is exactly what a review needs to see.
    """
    if limit <= 0:
        raise ValueError(f"invalid_limit: {limit}")
    if state not in ("all", "open", "ended"):
        raise ValueError(f"invalid_state: {state!r}; must be one of all, open, ended")
    days = max(1, min(days, _MAX_WINDOW_DAYS))
    effective_limit = min(limit, settings.query_row_limit)

    where_parts: list[sql.Composable] = [
        sql.SQL("COALESCE(e.ended_at, now()) >= now() - make_interval(days => %s)")
    ]
    params: list[Any] = [days]
    if state == "open":
        where_parts.append(sql.SQL("e.ended_at IS NULL"))
    elif state == "ended":
        where_parts.append(sql.SQL("e.ended_at IS NOT NULL"))
    if fault is not None:
        where_parts.append(sql.SQL("e.fault = %s"))
        params.append(fault)
    if only_unrated:
        where_parts.append(sql.SQL("v.verdict IS NULL"))
    params.append(effective_limit + 1)

    stmt = sql.SQL(
        """
        SELECT e.id AS episode_id, e.fault, e.subject,
               COALESCE(c.name, substring(e.subject from '\\[([^]]+)\\]'), e.subject)
                   AS affected,
               c.room, e.severity, e.started_at, e.last_seen_at, e.ended_at,
               e.peak_score, e.folded, e.externally_delivered,
               v.verdict, v.decided_at
        FROM episodes e
        {catalog}
        LEFT JOIN episode_verdicts v ON v.episode_id = e.id
        WHERE {where}
        ORDER BY e.started_at DESC
        LIMIT %s
        """
    ).format(catalog=_SUBJECT_SQL, where=sql.SQL(" AND ").join(where_parts))

    m = metrics_module.get()
    m.db_queries.labels(tool="list_episodes", table_used="episodes").inc()
    with m.db_query_duration.labels(tool="list_episodes").time():
        async with connection() as conn:
            rows = await (await conn.execute(stmt, params)).fetchall()

    truncated = len(rows) > effective_limit
    rows = rows[:effective_limit]
    return {
        "state": state,
        "days": days,
        "filters": {"fault": fault, "only_unrated": only_unrated},
        "limit": effective_limit,
        "row_count": len(rows),
        "truncated": truncated,
        "episodes": [_serialize(r) for r in rows],
    }


async def set_episode_verdict(
    *,
    settings: Settings,  # noqa: ARG001 — keep signature consistent with the other tools
    episode_id: int,
    verdict: str,
) -> dict[str, Any]:
    """Record ``verdict`` on one episode, overwriting any earlier one.

    One row per episode by primary key, so a second thought replaces the
    first instead of stacking beside it.
    """
    if verdict not in VERDICTS:
        raise ValueError(f"invalid_verdict: {verdict!r}; must be one of {', '.join(VERDICTS)}")

    m = metrics_module.get()
    m.db_queries.labels(tool="set_episode_verdict", table_used="episode_verdicts").inc()
    with m.db_query_duration.labels(tool="set_episode_verdict").time():
        async with write_connection() as conn:
            episode = await (
                await conn.execute(
                    "SELECT fault, subject FROM episodes WHERE id = %s", (episode_id,)
                )
            ).fetchone()
            if episode is None:
                raise ValueError(
                    f"unknown_episode: {episode_id}; call list_episodes to find a valid id"
                )
            written = await (
                await conn.execute(
                    """
                    INSERT INTO episode_verdicts (episode_id, verdict)
                    VALUES (%s, %s)
                    ON CONFLICT (episode_id)
                    DO UPDATE SET verdict = EXCLUDED.verdict, decided_at = now()
                    RETURNING episode_id, verdict, decided_at
                    """,
                    (episode_id, verdict),
                )
            ).fetchone()

    if written is None:  # INSERT … RETURNING always yields the row
        raise RuntimeError(f"verdict write for episode {episode_id} returned no row")
    return _serialize({**written, "fault": episode["fault"], "subject": episode["subject"]})
