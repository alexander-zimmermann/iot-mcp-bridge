"""Tests for presence: who was at home, and when the house was empty, over a window."""

from __future__ import annotations

import pytest

from lares_mcp_bridge.tools import presence

# The seeded day: Anna leaves at 07:00 and is back at 17:00, Ben is out from
# 09:00 to 12:00, Dora's first change is "away" at 10:00 and she is back at 11:00.
DAY = {"from_ts": "2026-09-01T00:00:00+00:00", "to_ts": "2026-09-02T00:00:00+00:00"}


async def test_each_person_is_a_few_spans_across_the_window(db_pool: None) -> None:
    result = await presence.query_presence(**DAY)

    assert result["persons"]["Anna"] == [
        # The state the window opens with is the last change before it.
        {"from": "2026-09-01T00:00:00+00:00", "to": "2026-09-01T07:00:00+00:00", "state": "home"},
        # A repeated "away" is the same span, not a new one.
        {"from": "2026-09-01T07:00:00+00:00", "to": "2026-09-01T17:00:00+00:00", "state": "away"},
        {"from": "2026-09-01T17:00:00+00:00", "to": "2026-09-02T00:00:00+00:00", "state": "home"},
    ]
    assert result["persons"]["Ben"] == [
        {"from": "2026-09-01T00:00:00+00:00", "to": "2026-09-01T09:00:00+00:00", "state": "home"},
        {"from": "2026-09-01T09:00:00+00:00", "to": "2026-09-01T12:00:00+00:00", "state": "away"},
        {"from": "2026-09-01T12:00:00+00:00", "to": "2026-09-02T00:00:00+00:00", "state": "home"},
    ]


async def test_a_person_without_a_state_before_the_window_opens_unknown(db_pool: None) -> None:
    result = await presence.query_presence(**DAY)

    assert result["persons"]["Dora"][0] == {
        "from": "2026-09-01T00:00:00+00:00",
        "to": "2026-09-01T10:00:00+00:00",
        "state": "unknown",
    }


async def test_a_person_who_reports_no_presence_is_named_apart(db_pool: None) -> None:
    """Cleo has a presence datapoint but never wrote it: nothing is known about
    her, so she is named as silent rather than guessed into a span. Anna's
    stream title is a Person datapoint and not presence at all."""
    result = await presence.query_presence(**DAY)

    assert sorted(result["persons"]) == ["Anna", "Ben", "Dora"]
    assert result["silent"] == ["Cleo"]


async def test_the_house_is_empty_only_while_everyone_is_known_to_be_away(
    db_pool: None,
) -> None:
    """Anna and Ben are both out from 09:00 to 12:00, but Dora is unknown until
    10:00 and back at 11:00: only that hour is provably an empty house."""
    result = await presence.query_presence(**DAY)

    assert result["house_empty"] == [
        {"from": "2026-09-01T10:00:00+00:00", "to": "2026-09-01T11:00:00+00:00"},
    ]


async def test_a_window_inside_one_span_is_that_one_state(db_pool: None) -> None:
    result = await presence.query_presence(
        from_ts="2026-09-01T13:00:00+00:00", to_ts="2026-09-01T15:00:00+00:00"
    )

    assert result["persons"]["Anna"] == [
        {"from": "2026-09-01T13:00:00+00:00", "to": "2026-09-01T15:00:00+00:00", "state": "away"},
    ]
    assert result["house_empty"] == []


@pytest.mark.parametrize(
    ("from_ts", "to_ts", "message"),
    [
        ("2026-09-02T00:00:00+00:00", "2026-09-01T00:00:00+00:00", "invalid_window"),
        ("2026-08-01T00:00:00+00:00", "2026-09-02T00:00:00+00:00", "window_too_long"),
        ("gestern", "2026-09-02T00:00:00+00:00", "invalid_timestamp"),
    ],
)
async def test_a_window_that_is_not_one_is_refused(
    db_pool: None, from_ts: str, to_ts: str, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        await presence.query_presence(from_ts=from_ts, to_ts=to_ts)
