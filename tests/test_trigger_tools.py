"""start_run tests against a respx-mocked lares-agent-trigger.

The tool carries the request to the trigger's `POST /api/runs` with the
shared key and hands back what the trigger answered; the trigger alone
decides whether a run starts. Nothing here needs the database.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import respx

from lares_mcp_bridge.config import Settings
from lares_mcp_bridge.tools import trigger

TRIGGER_URL = "http://lares-agent-trigger.agents.svc.cluster.local:8080"
KEY = "t" * 32


def _settings(trigger_url: str | None = None, trigger_key_file: str | None = None) -> Settings:
    return Settings(
        db_host="localhost",
        db_name="x",
        db_username="x",
        db_password="x",
        auth_enabled=False,
        nats_enabled=False,
        trigger_url=trigger_url,
        trigger_key_file=trigger_key_file,
    )


@pytest.fixture
def key_file(tmp_path: Path) -> Path:
    path = tmp_path / "trigger-api-key"
    path.write_text(f"{KEY}\n", encoding="utf-8")  # trailing newline, like a mounted Secret
    return path


@pytest_asyncio.fixture
async def trigger_client(key_file: Path) -> AsyncIterator[None]:
    await trigger.init(_settings(trigger_url=TRIGGER_URL, trigger_key_file=str(key_file)))
    try:
        yield
    finally:
        await trigger.close()


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(base_url=TRIGGER_URL, assert_all_called=False) as mock:
        yield mock


# ---------------------------------------------------------------- settings


def test_trigger_settings_must_be_set_together(key_file: Path) -> None:
    with pytest.raises(ValueError, match="MCP_TRIGGER_URL and MCP_TRIGGER_KEY_FILE"):
        _settings(trigger_url=TRIGGER_URL)
    with pytest.raises(ValueError, match="MCP_TRIGGER_URL and MCP_TRIGGER_KEY_FILE"):
        _settings(trigger_key_file=str(key_file))


def test_trigger_enabled_only_when_both_set(key_file: Path) -> None:
    assert _settings().trigger_enabled is False
    assert _settings(trigger_url=TRIGGER_URL, trigger_key_file=str(key_file)).trigger_enabled


# ---------------------------------------------------------------- lifecycle


async def test_start_run_refuses_when_not_initialised() -> None:
    with pytest.raises(RuntimeError, match="MCP_TRIGGER_URL"):
        await trigger.start_run("explain-episode", "15510")


async def test_init_rejects_empty_key_file(tmp_path: Path) -> None:
    empty = tmp_path / "trigger-api-key"
    empty.write_text("\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        await trigger.init(_settings(trigger_url=TRIGGER_URL, trigger_key_file=str(empty)))


# ---------------------------------------------------------------- start_run


async def test_an_episode_run_is_forwarded_with_the_key(
    trigger_client: None, router: respx.MockRouter
) -> None:
    answer = {
        "use_case": "explain-episode",
        "run_id": 17,
        "subject_key": "15510:message:20261002T142005Z",
        "status": "queued",
        "output": ["stored", "discord", "mail"],
    }
    router.post("/api/runs").mock(return_value=httpx.Response(202, json=answer))

    out = await trigger.start_run("explain-episode", "15510")

    assert out == answer
    request = router.calls.last.request
    assert request.headers["authorization"] == f"Bearer {KEY}"
    assert json.loads(request.content) == {"use_case": "explain-episode", "subject": "15510"}


async def test_a_schedule_run_is_sent_without_a_subject(
    trigger_client: None, router: respx.MockRouter
) -> None:
    router.post("/api/runs").mock(
        return_value=httpx.Response(
            202,
            json={"use_case": "propose-faults", "job_id": "b0b000000001", "status": "requested"},
        )
    )

    out = await trigger.start_run("propose-faults")

    assert out["status"] == "requested"
    assert json.loads(router.calls.last.request.content) == {"use_case": "propose-faults"}


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (404, "no episode 99999"),
        (409, "summarise-week is dormant: Waits for its skill."),
        (429, "explain-episode has spent its 10 runs today"),
    ],
)
async def test_a_refusal_carries_the_triggers_reason(
    trigger_client: None, router: respx.MockRouter, status: int, reason: str
) -> None:
    """The chat says why nothing started, in the trigger's words."""
    router.post("/api/runs").mock(return_value=httpx.Response(status, json={"error": reason}))

    with pytest.raises(ValueError, match=f"run_not_started: {reason}"):
        await trigger.start_run("explain-episode", "99999")


@pytest.mark.parametrize(
    "failure",
    [
        httpx.Response(503, json={"error": "the ledger or the harness did not answer"}),
        httpx.Response(502, text="Bad Gateway"),
        httpx.ConnectError("connection refused"),
    ],
    ids=["503", "no-json", "unreachable"],
)
async def test_a_trigger_that_does_not_answer_is_named(
    trigger_client: None, router: respx.MockRouter, failure: httpx.Response | Exception
) -> None:
    route = router.post("/api/runs")
    if isinstance(failure, Exception):
        route.mock(side_effect=failure)
    else:
        route.mock(return_value=failure)

    with pytest.raises(RuntimeError, match="trigger_unavailable"):
        await trigger.start_run("explain-episode", "15510")
