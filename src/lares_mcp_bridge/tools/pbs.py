"""Backup tools — what the Proxmox Backup Server holds and whether it was verified, read-only.

Three reads on the PBS REST API (``/api2/json``) under one API token with the
``Audit`` role on ``/``, the role the Prometheus exporter's token has: it sees
every datastore, snapshot, job and task, and may change none of them.

- ``list_pbs_datastores`` joins three answers per datastore: usage and the
  fill estimate (``/status/datastore-usage``), the garbage-collection job
  (``/admin/gc``) and the verify jobs (``/admin/verify``).
- ``list_pbs_snapshots`` lists one datastore's snapshots
  (``/admin/datastore/<store>/snapshots``), each with the state of its last
  verification.
- ``list_pbs_tasks`` reads the node's task log (``/nodes/localhost/tasks``).

PBS speaks epoch seconds; everything here leaves as ISO 8601 in UTC, like the
database tools' timestamps. A job that never ran has no last state, and that
is reported as ``None``, never as a success.

One shared httpx client, opened in the app lifespan like the wiki's; the
token secret arrives as a mounted Secret file. TLS is verified: the PBS
serves its ACME certificate on the name it is reached by.
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from ..config import Settings
from ..logging_setup import get_logger

log = get_logger(__name__)

_TIMEOUT_SECONDS = 15.0

_client: httpx.AsyncClient | None = None
_row_limit = 0


class PbsError(Exception):
    """PBS answered, but refused or could not do what was asked."""


async def init(settings: Settings) -> None:
    """Open the shared client with the token from the mounted file. Idempotent."""
    global _client, _row_limit
    if _client is not None:
        return
    url, token_id, token_file = settings.pbs_url, settings.pbs_token_id, settings.pbs_token_file
    if not url or not token_id or not token_file:
        raise ValueError("MCP_PBS_URL, MCP_PBS_TOKEN_ID and MCP_PBS_TOKEN_FILE are required")
    secret = Path(token_file).read_text(encoding="utf-8").strip()
    if not secret:
        raise ValueError(f"{token_file} is empty")
    _client = httpx.AsyncClient(
        base_url=f"{url.rstrip('/')}/api2/json",
        headers={"Authorization": f"PBSAPIToken={token_id}:{secret}"},
        timeout=_TIMEOUT_SECONDS,
    )
    _row_limit = settings.query_row_limit
    log.info("pbs_client_ready", url=url, token_id=token_id)


async def close() -> None:
    """Close and drop the shared client."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _require_client() -> httpx.AsyncClient:
    if _client is None:
        raise RuntimeError(
            "backup tools are disabled — set MCP_PBS_URL, MCP_PBS_TOKEN_ID and MCP_PBS_TOKEN_FILE"
        )
    return _client


async def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    """GET one API path and return its ``data``; a refusal raises PbsError with PBS's words."""
    response = await _require_client().get(path, params=params)
    if response.status_code in (401, 403):
        raise PbsError(
            f"PBS refused the token on {path} ({response.status_code})"
            " — it needs the Audit role on /"
        )
    if response.status_code < 500 and response.is_error:
        raise PbsError(f"PBS refused {path}: {_message(response)}")
    response.raise_for_status()
    return response.json()["data"]


def _message(response: httpx.Response) -> str:
    """PBS's own ``message``, or the raw status where the body carries none."""
    try:
        body = response.json()
    except ValueError:
        body = None
    if isinstance(body, dict) and isinstance(body.get("message"), str):
        return str(body["message"]).strip()
    return f"{response.status_code} {response.reason_phrase}"


def _at(epoch: int | None) -> str | None:
    return datetime.fromtimestamp(epoch, UTC).isoformat() if epoch is not None else None


def _cap(limit: int) -> int:
    if limit <= 0:
        raise ValueError(f"invalid_limit: {limit}")
    return min(limit, _row_limit)


def _last_run(job: dict[str, Any]) -> dict[str, Any]:
    """A job's schedule and its last and next run, as both job lists report them."""
    return {
        "schedule": job.get("schedule"),
        "last_run_state": job.get("last-run-state"),
        "last_run_ended_at": _at(job.get("last-run-endtime")),
        "next_run_at": _at(job.get("next-run")),
    }


async def list_pbs_datastores() -> dict[str, Any]:
    """Every datastore with its usage, its garbage collection and its verify jobs."""
    usage, gc_jobs, verify_jobs = await asyncio.gather(
        _get("/status/datastore-usage"), _get("/admin/gc"), _get("/admin/verify")
    )
    gc_by_store = {job["store"]: job for job in gc_jobs}
    datastores = []
    for entry in usage:
        store = entry["store"]
        gc = gc_by_store.get(store)
        datastores.append(
            {
                "name": store,
                "total_bytes": entry.get("total"),
                "used_bytes": entry.get("used"),
                "avail_bytes": entry.get("avail"),
                "estimated_full_at": _at(entry.get("estimated-full-date")),
                "error": entry.get("error"),
                "gc": None
                if gc is None
                else {
                    **_last_run(gc),
                    "removed_bytes": gc.get("removed-bytes"),
                    "pending_bytes": gc.get("pending-bytes"),
                    "still_bad_chunks": gc.get("still-bad"),
                },
                "verify_jobs": [
                    {
                        "id": job["id"],
                        **_last_run(job),
                        "last_run_upid": job.get("last-run-upid"),
                    }
                    for job in verify_jobs
                    if job["store"] == store
                ],
            }
        )
    return {"datastores": datastores}


async def list_pbs_snapshots(
    datastore: str,
    backup_type: str | None = None,
    backup_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """One datastore's snapshots, newest first, and how many passed, failed or never ran verify."""
    limit = _cap(limit)
    params = {
        key: value
        for key, value in (("backup-type", backup_type), ("backup-id", backup_id))
        if value is not None
    }
    entries = await _get(f"/admin/datastore/{quote(datastore, safe='')}/snapshots", params)
    entries.sort(key=lambda entry: entry["backup-time"], reverse=True)
    states = Counter(
        (entry.get("verification") or {}).get("state", "unverified") for entry in entries
    )
    return {
        "datastore": datastore,
        "counts": {state: states[state] for state in ("ok", "failed", "unverified")},
        "snapshot_count": len(entries),
        "truncated": len(entries) > limit,
        "snapshots": [_snapshot(entry) for entry in entries[:limit]],
    }


def _snapshot(entry: dict[str, Any]) -> dict[str, Any]:
    verification = entry.get("verification") or {}
    return {
        "backup_type": entry["backup-type"],
        "backup_id": entry["backup-id"],
        "backup_time": _at(entry["backup-time"]),
        "size_bytes": entry.get("size"),
        "verification": verification.get("state"),
        "verify_upid": verification.get("upid"),
        "protected": entry.get("protected", False),
        "owner": entry.get("owner"),
        "comment": entry.get("comment"),
    }


async def list_pbs_tasks(
    task_type: str | None = None,
    datastore: str | None = None,
    errors_only: bool = False,
    days: int = 7,
    limit: int = 50,
) -> dict[str, Any]:
    """The node's tasks of the last ``days``, newest first, with their end state."""
    if days <= 0:
        raise ValueError(f"invalid_days: {days}")
    limit = _cap(limit)
    params: dict[str, Any] = {
        "since": int(time.time()) - days * 86_400,
        # One more than asked, so a cut list says so.
        "limit": limit + 1,
    }
    if task_type is not None:
        params["typefilter"] = task_type
    if datastore is not None:
        params["store"] = datastore
    if errors_only:
        params["errors"] = 1
    entries = await _get("/nodes/localhost/tasks", params)
    return {
        "truncated": len(entries) > limit,
        "tasks": [_task(entry) for entry in entries[:limit]],
    }


def _task(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "upid": entry["upid"],
        "type": entry["worker_type"],
        "target": entry.get("worker_id"),
        "user": entry["user"],
        "started_at": _at(entry["starttime"]),
        "ended_at": _at(entry.get("endtime")),
        "running": entry.get("endtime") is None,
        "status": entry.get("status"),
    }
