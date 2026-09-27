"""Tests for reading episodes: the review list, and the evidence bundle an
explanation opens with."""

from __future__ import annotations

import pytest

from lares_mcp_bridge.tools import episodes, verdicts


async def _episode_id(fault: str, *, open_only: bool = False) -> int:
    result = await episodes.list_episodes(fault=fault, days=365)
    rows = [r for r in result["episodes"] if not open_only or r["ended_at"] is None]
    return int(rows[0]["episode_id"])


async def _episode_id_of(subject: str) -> int:
    result = await episodes.list_episodes(days=365)
    return int(next(r for r in result["episodes"] if r["subject"] == subject)["episode_id"])


async def test_list_episodes_resolves_subject_against_the_catalog(clean_verdicts: None) -> None:
    result = await episodes.list_episodes(days=365)
    by_subject = {r["subject"]: r for r in result["episodes"]}
    assert by_subject["knx [1/2/2]"]["affected"] == "Lighting.1F.Bedroom.Ceiling"
    assert by_subject["knx [1/2/2]"]["room"] == "Bedroom"
    # No group address in the subject — the subject itself is the best label.
    assert by_subject["ems boiler"]["affected"] == "ems boiler"


async def test_list_episodes_filters_by_state_and_window(clean_verdicts: None) -> None:
    open_only = await episodes.list_episodes(state="open", days=365)
    assert [r["fault"] for r in open_only["episodes"]] == ["silence"]
    assert all(r["ended_at"] is None for r in open_only["episodes"])

    ended_only = await episodes.list_episodes(state="ended", days=365)
    assert all(r["ended_at"] is not None for r in ended_only["episodes"])
    assert ended_only["row_count"] == 3

    # The window is an overlap, so the episode that started 5 hours ago and
    # the one that ran three days ago both fall inside a week.
    recent = await episodes.list_episodes(days=7)
    assert {r["fault"] for r in recent["episodes"]} == {"silence"}
    assert recent["row_count"] == 2


async def test_list_episodes_carries_the_newest_explanation(clean_verdicts: None) -> None:
    """One line per episode saying what the platform already said about it —
    the newest of the two explanations, so the escalation wins over the
    appearance, and its run id is what a verdict is then given on."""
    explained = await _episode_id("silence", open_only=True)
    listed = await episodes.list_episodes(days=365)
    by_id = {r["episode_id"]: r for r in listed["episodes"]}

    assert by_id[explained]["explanation_tldr"] == "Auch die Nachbarkanäle des Geräts schweigen."
    assert by_id[explained]["explanation_run_id"] is not None

    unexplained = await _episode_id("constancy")
    assert by_id[unexplained]["explanation_tldr"] is None
    assert by_id[unexplained]["explanation_run_id"] is None


async def test_list_episodes_can_narrow_to_the_unjudged_ones(clean_verdicts: None) -> None:
    episode_id = await _episode_id("silence", open_only=True)
    await verdicts.set_verdict(target="episode", verdict="real", episode_id=episode_id)

    unjudged = await episodes.list_episodes(days=365, only_unjudged=True)
    assert episode_id not in [r["episode_id"] for r in unjudged["episodes"]]
    assert all(r["verdict"] is None for r in unjudged["episodes"])


async def test_one_episode_reads_back_outside_the_default_window(clean_verdicts: None) -> None:
    """A verdict must be readable without guessing how wide the window has to
    be — the oldest seeded episode is 40 days back, far outside the default."""
    oldest = await _episode_id("constancy")
    await verdicts.set_verdict(target="episode", verdict="real", episode_id=oldest)

    named = await episodes.list_episodes(episode_id=oldest)
    assert named["row_count"] == 1
    assert named["episodes"][0]["verdict"] == "real"
    assert named["days"] is None


async def test_invalid_window_and_state_are_refused(clean_verdicts: None) -> None:
    with pytest.raises(ValueError, match="invalid_days"):
        await episodes.list_episodes(days=0)
    with pytest.raises(ValueError, match="open, ended"):
        await episodes.list_episodes(state="offen")  # type: ignore[arg-type]


async def test_get_episode_bundles_the_evidence_of_one_episode(clean_verdicts: None) -> None:
    """One call carries the episode, what it did, the channel it was measured
    on, that channel's neighbours, and what was already said about it."""
    episode_id = await _episode_id("silence", open_only=True)
    bundle = await episodes.get_episode(episode_id=episode_id)

    assert bundle["episode"]["episode_id"] == episode_id
    assert bundle["episode"]["fault"] == "silence"
    assert bundle["episode"]["affected"] == "Lighting.1F.Bedroom.Ceiling"

    # Chronological, so the trajectory reads forwards.
    assert [e["kind"] for e in bundle["events"]] == ["appeared", "escalated"]
    times = [o["time"] for o in bundle["observations"]]
    assert times == sorted(times)
    assert len(times) == 5
    assert bundle["observations"][-1]["score"] == pytest.approx(9.5)
    assert bundle["observations_truncated"] is False

    assert bundle["channel"]["ga"] == "1/2/2"
    assert bundle["channel"]["dpt"] == "1.001"
    # Same room, the channel itself excluded.
    assert {s["ga"] for s in bundle["siblings"]} == {"1/2/0", "1/2/1"}

    assert bundle["explanation"]["tldr"] == "Auch die Nachbarkanäle des Geräts schweigen."
    assert bundle["explanation"]["text"].startswith("Seit der Eskalation")
    assert bundle["explanation"]["run_id"] is not None


async def test_get_episode_carries_the_verdict_it_already_has(clean_verdicts: None) -> None:
    episode_id = await _episode_id("silence", open_only=True)
    await verdicts.set_verdict(target="episode", verdict="nonsense", episode_id=episode_id)

    bundle = await episodes.get_episode(episode_id=episode_id)
    assert bundle["episode"]["verdict"] == "nonsense"


async def test_get_episode_without_a_group_address_still_bundles(clean_verdicts: None) -> None:
    """A subject that names no channel has no catalog entry and no
    neighbours — an empty bundle, not an error."""
    boiler = await _episode_id_of("ems boiler")
    bundle = await episodes.get_episode(episode_id=boiler)

    assert bundle["episode"]["subject"] == "ems boiler"
    assert bundle["channel"] is None
    assert bundle["siblings"] == []
    assert bundle["events"] == []
    assert bundle["observations"] == []
    assert bundle["explanation"] is None


async def test_get_episode_on_an_unknown_id_is_a_precise_error(clean_verdicts: None) -> None:
    with pytest.raises(ValueError, match="unknown_episode: 999999"):
        await episodes.get_episode(episode_id=999999)
