"""NATS JetStream access for live "current state" reads.

The MCP tools read the last retained message per subject straight from
JetStream (`get_last_msg` / a `LAST_PER_SUBJECT` consumer) — no projector, no
Redis. This module owns a single shared connection (opened in the app lifespan,
like the DB pool) and normalises every NATS message into a small `StateMsg` so
the tool layer never touches nats-py types directly.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import nats
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, DeliverPolicy
from nats.js.errors import NotFoundError

from .config import Settings
from .logging_setup import get_logger

log = get_logger(__name__)

# A single shared connection + JetStream context, opened once in the lifespan.
_nc: NATSClient | None = None
_js: JetStreamContext | None = None

# Trailing fractional seconds beyond microsecond precision — NATS server times
# carry nanoseconds, which datetime.fromisoformat() cannot parse.
_NANOS_RE = re.compile(r"(\.\d{6})\d+")


@dataclass(frozen=True)
class StateMsg:
    """One NATS message reduced to what the live tools need."""

    subject: str
    data: bytes
    ts: datetime  # tz-aware UTC; JetStream stored time (or receipt time for tails)


async def init(settings: Settings) -> None:
    """Open the shared connection. Idempotent — a no-op once connected."""
    global _nc, _js
    if _nc is not None and _nc.is_connected:
        return
    kwargs: dict[str, Any] = {
        "servers": settings.nats_servers_list,
        "max_reconnect_attempts": -1,
        "reconnect_time_wait": 2,
        "connect_timeout": 10,
    }
    if settings.nats_nkey_seed_file:
        kwargs["nkeys_seed"] = settings.nats_nkey_seed_file
    _nc = await nats.connect(**kwargs)
    _js = _nc.jetstream()
    log.info("nats_connected", servers=settings.nats_servers_list)


async def close() -> None:
    """Drain and drop the shared connection."""
    global _nc, _js
    if _nc is not None:
        with contextlib.suppress(Exception):
            await _nc.drain()
        _nc = None
        _js = None


async def healthcheck() -> bool:
    return _nc is not None and _nc.is_connected


def _require_js() -> JetStreamContext:
    if _js is None:
        raise RuntimeError("NATS not initialised — call init() first")
    return _js


def _require_nc() -> NATSClient:
    if _nc is None:
        raise RuntimeError("NATS not initialised — call init() first")
    return _nc


async def last_msg(stream: str, subject: str) -> StateMsg | None:
    """Last retained message for an exact subject, or None if the stream holds none."""
    js = _require_js()
    try:
        raw = await js.get_last_msg(stream, subject)
    except NotFoundError:
        return None
    return StateMsg(
        subject=raw.subject or subject,
        data=raw.data or b"",
        ts=_parse_ts(raw.time),
    )


async def last_per_subject(stream: str, filter_subject: str, limit: int = 64) -> list[StateMsg]:
    """Last message of every subject matching ``filter_subject`` (one shot).

    Uses an ephemeral ``LAST_PER_SUBJECT`` pull consumer drained until it runs
    dry, capped at ``limit`` to bound the result for broad wildcards.
    """
    js = _require_js()
    config = ConsumerConfig(deliver_policy=DeliverPolicy.LAST_PER_SUBJECT)
    psub = await js.pull_subscribe(filter_subject, stream=stream, config=config)
    out: list[StateMsg] = []
    try:
        while len(out) < limit:
            try:
                batch = await psub.fetch(min(limit - len(out), 64), timeout=1.0)
            except TimeoutError:  # nats.errors.TimeoutError subclasses the builtin
                break
            for msg in batch:
                out.append(
                    StateMsg(subject=msg.subject, data=msg.data or b"", ts=_jetstream_ts(msg))
                )
                await msg.ack()
            if len(batch) < 64:
                break
    finally:
        with contextlib.suppress(Exception):
            await psub.unsubscribe()
    return out


async def tail(subject: str, duration_s: float, max_messages: int = 200) -> list[StateMsg]:
    """Collect live messages on ``subject`` for a bounded window via a core subscription."""
    nc = _require_nc()
    queue: asyncio.Queue[Any] = asyncio.Queue()

    async def _on_message(msg: Any) -> None:
        await queue.put(msg)

    sub = await nc.subscribe(subject, cb=_on_message)
    out: list[StateMsg] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + duration_s
    try:
        while len(out) < max_messages:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=remaining)
            except TimeoutError:  # nats.errors.TimeoutError subclasses the builtin
                break
            out.append(StateMsg(subject=msg.subject, data=msg.data or b"", ts=datetime.now(UTC)))
    finally:
        with contextlib.suppress(Exception):
            await sub.unsubscribe()
    return out


def _jetstream_ts(msg: Any) -> datetime:
    """Stored timestamp of a JetStream message, falling back to now()."""
    try:
        ts = msg.metadata.timestamp
    except Exception:  # noqa: BLE001 — metadata may be absent on edge cases
        return datetime.now(UTC)
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=UTC)
    return datetime.now(UTC)


def _parse_ts(value: str | datetime | None) -> datetime:
    """Parse a NATS RFC3339 timestamp (nanosecond precision) into tz-aware UTC."""
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = _NANOS_RE.sub(r"\1", value.strip().replace("Z", "+00:00"))
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return datetime.now(UTC)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
