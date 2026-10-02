"""Reading the episodes the detection chain holds: the review list, and the
evidence bundle behind one of them.

The mails Basalte sends are the prompt to review, not the archive — the
engine already knows every situation it caused, so the review happens
against this list. Each row carries the verdict it has and the newest thing
the platform said about it, so "what still needs judging" and "what was
already explained" are one read.

``get_episode`` is the other half: everything an explanation needs about one
episode in a single call — the row, its trajectory, the channel it was
measured on, that channel's neighbours and the last explanation. One call is
fewer tokens than four, and less room to invent between them.

Verdicts are written in :mod:`.verdicts`; nothing here writes.
"""

from __future__ import annotations

from typing import Any, Literal, get_args

from psycopg import sql

from .. import db
from .roles import classify, device_of
from .runs import SUBJECT_KEY_MATCH

EpisodeState = Literal["all", "open", "ended"]

# An episode's trajectory is one observation per evaluation tick, and each
# one stays in the model's context for the rest of its run; two days of hourly
# ticks say where it stands.
_OBSERVATION_LIMIT = 48
_SIBLING_LIMIT = 50

# `episode_view` resolves a subject against the catalog — the engine says
# what it names, the view turns that into a channel, a room and a label.
# The rule lives there once, for this server and the dashboard alike.
_EPISODE_COLUMNS = sql.SQL(
    """
    e.id AS episode_id, e.fault, e.subject, e.entity_kind, e.entity_ref,
    e.affected, e.room, e.channel_ga, e.channel_name,
    e.severity, e.started_at, e.last_seen_at, e.ended_at,
    e.peak_score, e.folded, e.externally_delivered,
    e.verdict, e.decided_at
    """
)

# An explanation is what a completed run produced: a run still going, capped
# before it started or failed has nothing to show beside the episode. The
# subject key names the episode in either of the two shapes `SUBJECT_KEY_MATCH`
# accepts, so bind the episode expression twice.
_NEWEST_EXPLANATION = sql.SQL(
    """
    SELECT r.id AS run_id, r.tldr, r.text, r.created_at
    FROM agent_runs r
    WHERE r.subject_kind = 'episode'
      AND {key_match}
      AND r.status = 'completed'
      AND r.tldr IS NOT NULL
    ORDER BY r.created_at DESC
    LIMIT 1
    """
)


async def list_episodes(
    *,
    state: EpisodeState = "all",
    episode_id: int | None = None,
    fault: str | None = None,
    days: int = 7,
    only_unjudged: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    """Episodes overlapping the last ``days``, newest first, each with the
    verdict it already carries and the newest explanation of it.

    The window is an overlap, not a start filter: an episode that began
    weeks ago and is still open is exactly what a review needs to see. Pass
    ``episode_id`` to read one episode back regardless of how old it is.
    """
    if state not in get_args(EpisodeState):
        raise ValueError(
            f"invalid_state: {state!r}; must be one of {', '.join(get_args(EpisodeState))}"
        )
    days = db.window_days(days)

    # A named episode is answered whatever its age — the window is for
    # browsing, not for hiding a verdict somebody just wrote.
    where_parts: list[sql.Composable] = []
    params: list[Any] = []
    if episode_id is not None:
        where_parts.append(sql.SQL("e.id = %s"))
        params.append(episode_id)
    else:
        where_parts.append(
            sql.SQL("COALESCE(e.ended_at, now()) >= now() - make_interval(days => %s)")
        )
        params.append(days)
    if state == "open":
        where_parts.append(sql.SQL("e.ended_at IS NULL"))
    elif state == "ended":
        where_parts.append(sql.SQL("e.ended_at IS NOT NULL"))
    if fault is not None:
        where_parts.append(sql.SQL("e.fault = %s"))
        params.append(fault)
    if only_unjudged:
        where_parts.append(sql.SQL("e.verdict IS NULL"))

    stmt = sql.SQL(
        """
        SELECT {columns},
               x.run_id AS explanation_run_id, x.tldr AS explanation_tldr
        FROM episode_view e
        LEFT JOIN LATERAL ({explanation}) x ON true
        WHERE {where}
        ORDER BY e.started_at DESC
        """
    ).format(
        columns=_EPISODE_COLUMNS,
        explanation=_NEWEST_EXPLANATION.format(
            key_match=SUBJECT_KEY_MATCH.format(key=sql.SQL("e.id::text"))
        ),
        where=sql.SQL(" AND ").join(where_parts),
    )
    result = await db.read(
        "list_episodes", "episodes", stmt, params, limit=limit, overflow="truncate"
    )

    return {
        "state": state,
        "days": None if episode_id is not None else days,
        "filters": {"episode_id": episode_id, "fault": fault, "only_unjudged": only_unjudged},
        "limit": result.limit,
        "row_count": len(result.rows),
        "truncated": result.truncated,
        "episodes": result.rows,
    }


async def get_episode(*, episode_id: int) -> dict[str, Any]:
    """Everything one episode is: the row, its events and observations, the
    catalog entry of its channel, the channels beside it in the same room,
    and the newest explanation of it.

    The episode row is the list row plus ``channel_ga`` and minus the two
    explanation columns — the bundle carries the explanation whole instead,
    text and all.
    """
    found = await db.lookup(
        "get_episode",
        "episodes",
        sql.SQL(
            """
            SELECT {columns}
            FROM episode_view e
            WHERE e.id = %s
            """
        ).format(columns=_EPISODE_COLUMNS),
        (episode_id,),
    )
    if not found:
        raise ValueError(f"unknown_episode: {episode_id}; call list_episodes to find a valid id")
    episode = found[0]

    # Bounded by its own primary key: at most one appeared, escalated and
    # ended per episode.
    events = await db.lookup(
        "get_episode",
        "episode_events",
        "SELECT kind, time, severity FROM episode_events WHERE episode_id = %s ORDER BY time",
        (episode_id,),
    )

    # Newest first so an overflow drops the oldest observations, reversed
    # back so the trajectory reads forwards.
    observed = await db.read(
        "get_episode",
        "episode_observations",
        """
        SELECT time, score, severity, value
        FROM episode_observations WHERE episode_id = %s
        ORDER BY time DESC
        """,
        (episode_id,),
        limit=_OBSERVATION_LIMIT,
        overflow="truncate",
    )

    channel, siblings, siblings_truncated = await _channel_and_siblings(
        episode["channel_ga"], episode["room"]
    )
    explanation = await _newest_explanation(episode_id)

    return {
        "episode": episode,
        "events": events,
        "observations": list(reversed(observed.rows)),
        "observations_truncated": observed.truncated,
        "channel": channel,
        "siblings": siblings,
        "siblings_truncated": siblings_truncated,
        "explanation": explanation,
    }


async def _channel_and_siblings(
    channel_ga: str | None, room: str | None
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], bool]:
    """The catalog entry of the channel a fault was measured on, and the
    other channels of its room.

    A fault measured on a room or a plant has no channel of its own but
    still has a room, so it gets the neighbours without the entry. One the
    catalog cannot place has neither — an empty bundle, not an error.
    """
    entry = None
    if channel_ga is not None:
        found = await db.lookup(
            "get_episode",
            "ga_catalog",
            "SELECT ga, name, room, function, dpt, description FROM ga_catalog WHERE ga = %s",
            (channel_ga,),
        )
        entry = found[0] if found else None
    if room is None:
        return entry, [], False

    # The whole room, since a command is only known as one against its
    # device's other datapoints; a room has a few hundred channels at most.
    siblings = await db.lookup(
        "get_episode",
        "ga_catalog",
        """
        SELECT ga, name, function, dpt, description
        FROM ga_catalog WHERE room = %s AND ga IS DISTINCT FROM %s
        ORDER BY name
        """,
        (room, channel_ga),
    )
    # A command carries the last order sent, not what its device does; what
    # is left leads with the channel's own device, where there is one.
    names = [row["name"] for row in siblings]
    own = entry["name"] if entry else None
    roles = classify(names, [*names, own] if own else names)
    device = device_of(own) if own else None
    reported = [row for row in siblings if roles[row["name"]] != "command"]
    reported.sort(key=lambda row: device_of(row["name"]) != device)
    return entry, reported[:_SIBLING_LIMIT], len(reported) > _SIBLING_LIMIT


async def _newest_explanation(episode_id: int) -> dict[str, Any] | None:
    """The last thing the platform said about this episode, or None."""
    rows = await db.lookup(
        "get_episode",
        "agent_runs",
        _NEWEST_EXPLANATION.format(key_match=SUBJECT_KEY_MATCH.format(key=sql.Placeholder())),
        (str(episode_id), str(episode_id)),
    )
    return rows[0] if rows else None
