"""Storage tools — what the house's S3 object store holds and how fresh it is, read-only.

The object store keeps the database and volume backups and the cold archive;
two of its buckets, the archives, exist nowhere else. These tools read
listings only — keys, sizes, timestamps — under a key whose policy allows
``s3:ListAllMyBuckets`` and ``s3:ListBucket`` and nothing else: no object is
ever read, written or deleted. They speak plain S3 (``ListBuckets``,
``ListObjectsV2``), so a change of the service behind the endpoint leaves
them as they are.

S3 lists keys in lexicographic order, not by time, so the newest object of a
bucket or a prefix is known only once all of it is listed: both tools page
through the whole listing and sort it themselves. ``list_s3_objects`` also
groups the listing one ``/`` below the prefix, which is how a stream that
stopped writing shows: its newest object is older than its neighbours'.

boto3 is synchronous; every call runs in a worker thread. One shared client,
opened in the app lifespan like the wiki's; both key halves arrive as
mounted Secret files.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from ..config import Settings
from ..logging_setup import get_logger

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

log = get_logger(__name__)

# SigV4 needs a region; a single-site store answers to any.
_REGION = "us-east-1"

_client: S3Client | None = None
_row_limit = 0


class S3Error(Exception):
    """The object store answered, but refused what was asked."""


@dataclass(frozen=True)
class _Object:
    key: str
    size: int
    last_modified: datetime


def _read_secret(path: str) -> str:
    value = Path(path).read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError(f"{path} is empty")
    return value


async def init(settings: Settings) -> None:
    """Open the shared client with both key halves from their files. Idempotent."""
    global _client, _row_limit
    if _client is not None:
        return
    url = settings.s3_endpoint_url
    access_file, secret_file = settings.s3_access_key_file, settings.s3_secret_key_file
    if not url or not access_file or not secret_file:
        raise ValueError(
            "MCP_S3_ENDPOINT_URL, MCP_S3_ACCESS_KEY_FILE and MCP_S3_SECRET_KEY_FILE are required"
        )
    _client = boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id=_read_secret(access_file),
        aws_secret_access_key=_read_secret(secret_file),
        region_name=_REGION,
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            connect_timeout=5,
            read_timeout=15,
            retries={"max_attempts": 2},
        ),
    )
    _row_limit = settings.query_row_limit
    log.info("s3_client_ready", url=url)


async def close() -> None:
    """Close and drop the shared client."""
    global _client
    if _client is not None:
        _client.close()
        _client = None


def _require_client() -> S3Client:
    if _client is None:
        raise RuntimeError(
            "storage tools are disabled — set MCP_S3_ENDPOINT_URL, MCP_S3_ACCESS_KEY_FILE"
            " and MCP_S3_SECRET_KEY_FILE"
        )
    return _client


def _refusal(exc: ClientError, bucket: str | None = None) -> S3Error:
    """The store's error code and message, with what to do about the usual ones."""
    error = exc.response.get("Error", {})
    code, message = error.get("Code", "Unknown"), error.get("Message", str(exc))
    hint = {
        "NoSuchBucket": f" — no bucket {bucket!r}; list_s3_buckets names them",
        "AccessDenied": " — the key's policy needs s3:ListAllMyBuckets and s3:ListBucket",
    }.get(code, "")
    return S3Error(f"{code}: {message}{hint}")


def _list_objects(client: S3Client, bucket: str, prefix: str) -> list[_Object]:
    """Every object under ``prefix``, in the store's (lexicographic) order."""
    objects: list[_Object] = []
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        objects.extend(
            _Object(entry["Key"], entry["Size"], entry["LastModified"])
            for entry in page.get("Contents", [])
        )
    return objects


async def _listing(bucket: str, prefix: str = "") -> list[_Object]:
    client = _require_client()
    try:
        return await asyncio.to_thread(_list_objects, client, bucket, prefix)
    except ClientError as exc:
        raise _refusal(exc, bucket) from exc
    except BotoCoreError as exc:
        raise S3Error(f"the object store could not be reached: {exc}") from exc


def _summary(objects: Iterable[_Object]) -> dict[str, Any]:
    """Count, size, the newest object and the oldest time of a listing."""
    listed = list(objects)
    newest = max(listed, key=lambda obj: obj.last_modified, default=None)
    oldest = min((obj.last_modified for obj in listed), default=None)
    return {
        "object_count": len(listed),
        "size_bytes": sum(obj.size for obj in listed),
        "newest_key": newest.key if newest else None,
        "newest_at": newest.last_modified.isoformat() if newest else None,
        "oldest_at": oldest.isoformat() if oldest else None,
    }


async def list_s3_buckets() -> dict[str, Any]:
    """Every bucket with its object count, size and newest object, in name order."""
    client = _require_client()
    try:
        answer = await asyncio.to_thread(client.list_buckets)
    except ClientError as exc:
        raise _refusal(exc) from exc
    except BotoCoreError as exc:
        raise S3Error(f"the object store could not be reached: {exc}") from exc
    buckets = []
    # One bucket at a time: a full listing is the store's heaviest read.
    for bucket in sorted(answer.get("Buckets", []), key=lambda entry: entry.get("Name", "")):
        name = bucket.get("Name", "")
        created = bucket.get("CreationDate")
        buckets.append(
            {
                "name": name,
                "created_at": created.isoformat() if created else None,
                **_summary(await _listing(name)),
            }
        )
    return {"buckets": buckets}


async def list_s3_objects(bucket: str, prefix: str = "", limit: int = 100) -> dict[str, Any]:
    """The objects under ``prefix``, newest first, and the next level of prefixes below it."""
    if limit <= 0:
        raise ValueError(f"invalid_limit: {limit}")
    limit = min(limit, _row_limit)
    objects = await _listing(bucket, prefix)
    children: dict[str, list[_Object]] = {}
    for obj in objects:
        head, separator, _ = obj.key[len(prefix) :].partition("/")
        if separator:
            children.setdefault(f"{prefix}{head}/", []).append(obj)
    newest_first = sorted(objects, key=lambda obj: obj.last_modified, reverse=True)
    return {
        "bucket": bucket,
        "prefix": prefix,
        **_summary(objects),
        "children": [
            {"prefix": child, **_summary(members)} for child, members in sorted(children.items())
        ],
        "truncated": len(objects) > limit,
        "objects": [
            {
                "key": obj.key,
                "size_bytes": obj.size,
                "last_modified": obj.last_modified.isoformat(),
            }
            for obj in newest_first[:limit]
        ],
    }
