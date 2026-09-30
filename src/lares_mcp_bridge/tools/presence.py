"""Who was at home, and when the house was empty, over a window.

Each person the house knows has one KNX datapoint ``Person.<name>.Präsenz``
(DPT 1.011, 1 = at home), written only when it changes. ``query_knx_events``
gives those changes raw; a question like "was anybody home while the freezer
drew power?" needs them folded: a few spans per person, and the spans in
which everybody was away.

The state a window opens with is the last change before it. Changes are rare,
so the look back is a month, never unbounded. A person with a change in that
month or in the window is listed, ``unknown`` until the first state is known;
a listed person who is unknown keeps the house from being provably empty. A
person with no change in either reports nothing at all, and is named as silent
instead of being listed or guessed.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Literal, NamedTuple

from psycopg import sql

from .. import db

State = Literal["home", "away", "unknown"]

_MAX_WINDOW = timedelta(days=31)
_LOOKBACK = timedelta(days=31)
_PREFIX, _SUFFIX = "Person.", ".Präsenz"

# Every presence datapoint of the catalog, with its changes in the look back
# and the window; a datapoint without any comes back once, with NULL time.
# `%%`: the statement is formatted with parameters, so a literal `%` doubles.
_PRESENCE_SQL = sql.SQL(
    """
    WITH presence AS (
        SELECT ga, name FROM ga_catalog
        WHERE function = 'Person' AND name LIKE 'Person.%%.Präsenz'
    ),
    opening AS (
        SELECT DISTINCT ON (k.ga) k.ga, k.time, k.value
        FROM knx k JOIN presence p USING (ga)
        WHERE k.time >= %s AND k.time < %s
        ORDER BY k.ga, k.time DESC
    ),
    inside AS (
        SELECT k.ga, k.time, k.value
        FROM knx k JOIN presence p USING (ga)
        WHERE k.time >= %s AND k.time < %s
    )
    SELECT p.name, x.time, x.value
    FROM presence p
    LEFT JOIN (SELECT * FROM opening UNION ALL SELECT * FROM inside) x USING (ga)
    ORDER BY p.name, x.time
    """
)


class Span(NamedTuple):
    """One stretch of time in one state."""

    start: datetime
    end: datetime
    state: State


async def query_presence(from_ts: str | datetime, to_ts: str | datetime) -> dict[str, Any]:
    """Spans of ``home`` / ``away`` / ``unknown`` per person, the empty-house spans,
    and the people who report no presence."""
    start, end = _parse(from_ts, "from_ts"), _parse(to_ts, "to_ts")
    if end <= start:
        raise ValueError(f"invalid_window: to_ts {end.isoformat()} is not after from_ts")
    if end - start > _MAX_WINDOW:
        raise ValueError(
            f"window_too_long: {end - start} exceeds {_MAX_WINDOW.days} days; narrow the window"
        )

    rows = await db.lookup(
        "query_presence",
        "knx",
        _PRESENCE_SQL,
        (start - _LOOKBACK, start, start, end),
    )
    changes: dict[str, list[tuple[datetime, State]]] = {}
    silent: list[str] = []
    for row in rows:
        person = row["name"].removeprefix(_PREFIX).removesuffix(_SUFFIX)
        if row["time"] is None:
            silent.append(person)
            continue
        state: State = "home" if row["value"] else "away"
        changes.setdefault(person, []).append((datetime.fromisoformat(row["time"]), state))

    persons = {person: _spans(points, start, end) for person, points in changes.items()}
    return {
        "from_ts": start.isoformat(),
        "to_ts": end.isoformat(),
        "persons": {person: [_render(span) for span in spans] for person, spans in persons.items()},
        "house_empty": [
            {"from": span.start.isoformat(), "to": span.end.isoformat()}
            for span in _house_empty(list(persons.values()), end)
        ],
        "silent": silent,
    }


def _spans(points: list[tuple[datetime, State]], start: datetime, end: datetime) -> list[Span]:
    """Fold one person's changes into spans covering the whole window.

    A change before the window is the state it opens with; without one the
    window opens ``unknown`` until the first change inside it.
    """
    spans: list[Span] = []
    current: State = "unknown"
    since = start
    for when, state in points:
        if when <= start:
            current = state
            continue
        if state == current:
            continue
        spans.append(Span(since, when, current))
        since, current = when, state
    spans.append(Span(since, end, current))
    return spans


def _house_empty(persons: list[list[Span]], end: datetime) -> list[Span]:
    """The spans in which every listed person was known to be away."""
    if not persons:
        return []
    cuts = sorted({span.start for spans in persons for span in spans})
    empty: list[Span] = []
    for index, begin in enumerate(cuts):
        finish = cuts[index + 1] if index + 1 < len(cuts) else end
        if all(_state_at(spans, begin) == "away" for spans in persons):
            if empty and empty[-1].end == begin:
                empty[-1] = empty[-1]._replace(end=finish)
            else:
                empty.append(Span(begin, finish, "away"))
    return empty


def _state_at(spans: list[Span], moment: datetime) -> State:
    return next(span.state for span in spans if span.start <= moment < span.end)


def _render(span: Span) -> dict[str, str]:
    return {"from": span.start.isoformat(), "to": span.end.isoformat(), "state": span.state}


def _parse(value: str | datetime, field: str) -> datetime:
    """An aware UTC datetime; a naive one is read as UTC, as the database reads it."""
    try:
        moment = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid_timestamp: {field} {value!r} is not ISO 8601") from exc
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)
