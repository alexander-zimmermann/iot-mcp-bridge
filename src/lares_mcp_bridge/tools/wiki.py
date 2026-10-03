"""Wiki tools — search, browse and read the house wiki (Wiki.js), and write one page.

The wiki holds the durable, human-written knowledge about the house: device
and vendor references, how-it-works notes, digests of external docs. Wiki.js
renders client-side, so without this an agent fetching a page sees a title
and nothing else.

Three reads under one key. ``search_wiki`` and ``list_wiki_pages`` use the
GraphQL API (``pages.search`` / ``pages.list``), which filter by
``read:pages``. Page *content* takes a different route: Wiki.js 2.x guards the
GraphQL ``pages.single`` / ``pages.singleByPath`` queries with ``manage:pages``
— an edit permission — so a read-only key never gets content that way. The
one read-only route to a page's source is the source view, ``GET
/s/<locale>/<path>`` (``read:source``), which embeds the raw source escaped in
a ``<code v-pre>`` element; ``get_wiki_page`` reads it from there. The read
key's group therefore needs exactly ``read:pages`` and ``read:source``.

One write under a second key, whose group needs ``read:pages`` and
``write:pages``: ``update_wiki_page`` looks the path up in the configured
locale through ``pages.list`` and creates the page or updates it. An update
carries the page's description, tags and publish flag over, because Wiki.js
resets whatever an update leaves out — an omitted flag unpublishes the page.
A publish window set in the editor is the one thing it cannot carry over:
``pages.list`` does not return it, so an update clears it. Every update
leaves the previous revision in the page's history. A write answers with the
raw page row, which holds ``localeCode`` and no ``locale``; selecting
``locale`` there fails the answer after the write has gone through, so the
locale reported back is the configured one.

One shared httpx client per key, opened in the app lifespan like the DB pool
and the NATS connection; each Bearer token arrives as a mounted Secret file.
"""

from __future__ import annotations

import html
import re
from pathlib import Path
from typing import Any

import httpx

from ..config import Settings
from ..logging_setup import get_logger

log = get_logger(__name__)

_TIMEOUT_SECONDS = 15.0

# source.pug: ``code(v-pre)= page.content`` — escaped, so the body never holds </code>.
_SOURCE_RE = re.compile(r"<code v-pre>(.*?)</code>", re.DOTALL)

_SEARCH_QUERY = """\
query ($query: String!) {
  pages {
    search(query: $query) {
      totalHits
      results { id path locale title description }
    }
  }
}"""

_LIST_QUERY = """\
query {
  pages {
    list(orderBy: PATH) {
      id path locale title description contentType tags createdAt updatedAt
    }
  }
}"""

_WRITE_LIST_QUERY = """\
query ($locale: String!) {
  pages {
    list(locale: $locale) { id path locale description isPublished tags }
  }
}"""

_WRITE_RESULT_FIELDS = """\
      responseResult { succeeded slug message }
      page { id path title updatedAt }"""

_CREATE_MUTATION = f"""\
mutation ($content: String!, $locale: String!, $path: String!, $title: String!) {{
  pages {{
    create(content: $content, description: "", editor: "markdown", isPublished: true,
           isPrivate: false, locale: $locale, path: $path, tags: [], title: $title) {{
{_WRITE_RESULT_FIELDS}
    }}
  }}
}}"""

_UPDATE_MUTATION = f"""\
mutation ($id: Int!, $content: String!, $title: String!, $description: String,
          $isPublished: Boolean!, $tags: [String]!) {{
  pages {{
    update(id: $id, content: $content, title: $title, description: $description,
           isPublished: $isPublished, tags: $tags) {{
{_WRITE_RESULT_FIELDS}
    }}
  }}
}}"""

_client: httpx.AsyncClient | None = None
_writer: httpx.AsyncClient | None = None
_write_locale: str | None = None


class WikiError(Exception):
    """The wiki answered, but not with what was asked for."""


def _open(url: str, token_file: str) -> httpx.AsyncClient:
    """A client on ``url`` with the Bearer token from the mounted file."""
    token = Path(token_file).read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError(f"{token_file} is empty")
    return httpx.AsyncClient(
        base_url=url.rstrip("/"),
        headers={"Authorization": f"Bearer {token}"},
        timeout=_TIMEOUT_SECONDS,
    )


async def init(settings: Settings) -> None:
    """Open the shared read client. Idempotent."""
    global _client
    if _client is not None:
        return
    url, token_file = settings.wikijs_url, settings.wikijs_token_file
    if not url or not token_file:
        raise ValueError("MCP_WIKIJS_URL and MCP_WIKIJS_TOKEN_FILE are required")
    _client = _open(url, token_file)
    log.info("wikijs_client_ready", url=url)


async def init_writer(settings: Settings) -> None:
    """Open the shared write client and note the locale writes go to. Idempotent."""
    global _writer, _write_locale
    if _writer is not None:
        return
    url, token_file = settings.wikijs_url, settings.wikijs_write_token_file
    if not url or not token_file:
        raise ValueError("MCP_WIKIJS_URL and MCP_WIKIJS_WRITE_TOKEN_FILE are required")
    _writer = _open(url, token_file)
    _write_locale = settings.wikijs_locale
    log.info("wikijs_writer_ready", url=url, locale=_write_locale)


async def close() -> None:
    """Close and drop both shared clients."""
    global _client, _writer, _write_locale
    for client in (_client, _writer):
        if client is not None:
            await client.aclose()
    _client = _writer = _write_locale = None


def _require_client() -> httpx.AsyncClient:
    if _client is None:
        raise RuntimeError("wiki tools are disabled — set MCP_WIKIJS_URL and MCP_WIKIJS_TOKEN_FILE")
    return _client


def _require_writer() -> tuple[httpx.AsyncClient, str]:
    if _writer is None or _write_locale is None:
        raise RuntimeError(
            "wiki writes are disabled — set MCP_WIKIJS_URL and MCP_WIKIJS_WRITE_TOKEN_FILE"
        )
    return _writer, _write_locale


async def _graphql(
    query: str, variables: dict[str, Any] | None = None, *, client: httpx.AsyncClient | None = None
) -> dict[str, Any]:
    """Run one query and return its ``pages`` payload; GraphQL errors raise WikiError."""
    response = await (client or _require_client()).post(
        "/graphql", json={"query": query, "variables": variables or {}}
    )
    response.raise_for_status()
    body: dict[str, Any] = response.json()
    if body.get("errors"):
        messages = "; ".join(str(err["message"]) for err in body["errors"])
        raise WikiError(f"wiki query failed: {messages}")
    pages: dict[str, Any] = body["data"]["pages"]
    return pages


async def _list_entries() -> list[dict[str, Any]]:
    """Every ``pages.list`` row the key may read, ordered by path."""
    entries: list[dict[str, Any]] = (await _graphql(_LIST_QUERY))["list"]
    return entries


async def _source(locale: str, path: str) -> str:
    """Raw source of one page from the ``/s/`` view — the read-only route to content."""
    response = await _require_client().get(f"/s/{locale}/{path}")
    if response.status_code == 403:
        raise WikiError(
            f"the wiki key may not read the source of {path!r} — its group needs read:source"
        )
    response.raise_for_status()
    found = _SOURCE_RE.search(response.text)
    if found is None:
        raise WikiError(
            f"source view of {path!r} has no <code v-pre> block — Wiki.js layout changed?"
        )
    return html.unescape(found.group(1))


def _summary(entry: dict[str, Any]) -> dict[str, Any]:
    """A ``pages.list`` row reduced to the browse view."""
    return {
        "id": entry["id"],
        "path": entry["path"],
        "locale": entry["locale"],
        "title": entry["title"],
        "description": entry["description"],
        "updated_at": entry["updatedAt"],
    }


async def search_wiki(query: str) -> dict[str, Any]:
    """``pages.search``: title/description/path hits among pages the key may read."""
    result = (await _graphql(_SEARCH_QUERY, {"query": query}))["search"]
    return {
        "query": query,
        "total_hits": result["totalHits"],
        "results": [
            {
                "id": int(hit["id"]),  # search engines index the page id as a string
                "path": hit["path"],
                "locale": hit["locale"],
                "title": hit["title"],
                "description": hit["description"],
            }
            for hit in result["results"]
        ],
    }


async def list_wiki_pages() -> dict[str, Any]:
    """``pages.list`` ordered by path — every page the key may read."""
    pages = [_summary(entry) for entry in await _list_entries()]
    return {"count": len(pages), "pages": pages}


async def get_wiki_page(path: str | None = None, page_id: int | None = None) -> dict[str, Any]:
    """One page in full: its list row plus the raw source from the ``/s/`` view.

    The page is resolved through ``pages.list`` first, so the locale the
    source view needs always comes from the page itself.
    """
    if page_id is not None and path is None:
        return await _read_page("id", page_id)
    if path is not None and page_id is None:
        return await _read_page("path", path.strip("/"))
    raise ValueError("pass exactly one of path or page_id")


async def _read_page(field: str, value: object) -> dict[str, Any]:
    """Resolve one list row by ``field == value``, then fetch its source."""
    # pages.list, not pages.single — the latter is guarded by manage:pages.
    ref = f"{field} {value!r}"
    matches = [entry for entry in await _list_entries() if entry[field] == value]
    if not matches:
        raise WikiError(f"no readable wiki page with {ref} — try search_wiki or list_wiki_pages")
    if len(matches) > 1:
        candidates = ", ".join(f"id {entry['id']} ({entry['locale']})" for entry in matches)
        raise WikiError(f"{ref} exists in several locales: {candidates} — call again with page_id")
    entry = matches[0]
    content = await _source(entry["locale"], entry["path"])
    return {
        **_summary(entry),
        "created_at": entry["createdAt"],
        "content_type": entry["contentType"],
        "tags": entry["tags"],
        "content": content,
    }


async def update_wiki_page(path: str, content: str, title: str) -> dict[str, Any]:
    """Create the page at ``path`` in the write locale, or update it where it exists.

    A refusal Wiki.js reports in its ``responseResult`` — a path the key may
    not write, an empty content, an illegal path — raises WikiError with the
    wiki's own slug and message.
    """
    client, locale = _require_writer()
    path = path.strip("/")
    entries = (await _graphql(_WRITE_LIST_QUERY, {"locale": locale}, client=client))["list"]
    existing = next((entry for entry in entries if entry["path"] == path), None)
    if existing is None:
        action, done = "create", "created"
        variables: dict[str, Any] = {
            "content": content,
            "locale": locale,
            "path": path,
            "title": title,
        }
        mutation = _CREATE_MUTATION
    else:
        action, done = "update", "updated"
        variables = {
            "id": existing["id"],
            "content": content,
            "title": title,
            "description": existing["description"],
            "isPublished": existing["isPublished"],
            "tags": existing["tags"],
        }
        mutation = _UPDATE_MUTATION
    result = (await _graphql(mutation, variables, client=client))[action]
    status = result["responseResult"]
    if not status["succeeded"]:
        raise WikiError(f"wiki refused to {action} {path!r}: {status['slug']}: {status['message']}")
    page = result["page"]
    log.info("wiki_page_written", action=action, path=path, locale=locale, page_id=page["id"])
    return {
        "action": done,
        "id": page["id"],
        "path": page["path"],
        "locale": locale,
        "title": page["title"],
        "updated_at": page["updatedAt"],
    }
