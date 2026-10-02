"""Starting a use case now: the request carried to lares-agent-trigger.

The trigger decides alone whether and how a run starts — it claims the
ledger row, counts the day's budget and delivers the output where the use
case says. This module only carries the request to the trigger's
``POST /api/runs`` with the key both pods share, and hands back what the
trigger answered. A refusal comes back as an error in the trigger's own
words, so the chat can say why nothing started.

One shared httpx client, opened in the app lifespan like the wiki's; the key
arrives as a mounted Secret file.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx

from ..config import Settings
from ..logging_setup import get_logger

log = get_logger(__name__)

# The trigger answers once the row is claimed; the run itself goes on without us.
_TIMEOUT_SECONDS = 30.0

_client: httpx.AsyncClient | None = None


async def init(settings: Settings) -> None:
    """Open the shared client with the key from the mounted file. Idempotent."""
    global _client
    if _client is not None:
        return
    url, key_file = settings.trigger_url, settings.trigger_key_file
    if not url or not key_file:
        raise ValueError("MCP_TRIGGER_URL and MCP_TRIGGER_KEY_FILE are required")
    key = Path(key_file).read_text(encoding="utf-8").strip()
    if not key:
        raise ValueError(f"{key_file} is empty")
    _client = httpx.AsyncClient(
        base_url=url.rstrip("/"),
        headers={"Authorization": f"Bearer {key}"},
        timeout=_TIMEOUT_SECONDS,
    )
    log.info("trigger_client_ready", url=url)


async def close() -> None:
    """Close and drop the shared client."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _require_client() -> httpx.AsyncClient:
    if _client is None:
        raise RuntimeError("start_run is disabled — set MCP_TRIGGER_URL and MCP_TRIGGER_KEY_FILE")
    return _client


async def start_run(use_case: str, subject: str | None = None) -> dict[str, Any]:
    """Ask the trigger to start ``use_case`` now, on ``subject`` where it runs on one."""
    body: dict[str, Any] = {"use_case": use_case}
    if subject is not None:
        body["subject"] = subject
    try:
        response = await _require_client().post("/api/runs", json=body)
    except httpx.HTTPError as exc:
        raise RuntimeError(f"trigger_unavailable: {exc}") from exc
    reason = _reason(response)
    if response.status_code >= 500:
        raise RuntimeError(f"trigger_unavailable: {reason}")
    if response.status_code >= 400:
        raise ValueError(f"run_not_started: {reason}")
    answer: dict[str, Any] = response.json()
    log.info("run_started", use_case=use_case, subject=subject, answer=answer)
    return answer


def _reason(response: httpx.Response) -> str:
    """The trigger's ``error``, or the raw status where the body carries none."""
    try:
        body = response.json()
    except ValueError:
        body = None
    if isinstance(body, dict) and isinstance(body.get("error"), str):
        return str(body["error"])
    return f"{response.status_code} {response.reason_phrase}"
