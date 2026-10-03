"""Use-case memory: the working notes of one use case, read here, appended through the trigger.

``agent_memory`` holds one row per use case, one entry per line. The trigger
is its only writer: an appended line goes to the trigger's
``POST /api/memory``, which keeps the row to its bound, about 8 KB, by
dropping the oldest lines. A line break inside the appended text would let
that cut leave half an entry behind, so it is refused here. The read is a
plain SELECT with the read role; a use case that has written nothing yet has
no row and reads as empty.
"""

from __future__ import annotations

from typing import Any

from .. import db
from ..logging_setup import get_logger
from . import trigger

log = get_logger(__name__)


async def get_memory(use_case: str) -> dict[str, Any]:
    """The use case's memory, oldest line first, and how many bytes it takes."""
    rows = await db.lookup(
        "get_memory",
        "agent_memory",
        """
        SELECT use_case, text, octet_length(text) AS bytes, updated_at
        FROM agent_memory WHERE use_case = %s
        """,
        (use_case,),
    )
    if not rows:
        return {"use_case": use_case, "text": "", "bytes": 0, "updated_at": None}
    return rows[0]


async def append_memory(use_case: str, line: str) -> dict[str, Any]:
    """Have the trigger append ``line`` to the use case's memory; what the trigger answered."""
    if "\n" in line.strip():
        raise ValueError("invalid_line: one line, without line breaks")
    answer = await trigger.post(
        "/api/memory", {"use_case": use_case, "text": line}, refusal="memory_not_appended"
    )
    log.info("memory_appended", use_case=use_case, answer=answer)
    return answer
