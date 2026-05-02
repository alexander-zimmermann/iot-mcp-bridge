"""Prometheus metrics registry and a tiny HTTP server exposing /metrics."""

from __future__ import annotations

import asyncio
import contextlib
import logging

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)

logger = logging.getLogger(__name__)


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()

        self.tool_calls = Counter(
            "iot_mcp_bridge_tool_calls_total",
            "MCP tool invocations partitioned by tool, OIDC subject, and outcome.",
            ["tool", "sub", "outcome"],
            registry=self.registry,
        )
        self.db_queries = Counter(
            "iot_mcp_bridge_db_queries_total",
            "Database queries issued by the MCP tools.",
            ["tool", "table_used"],
            registry=self.registry,
        )
        self.db_query_duration = Histogram(
            "iot_mcp_bridge_db_query_duration_seconds",
            "Wall-clock duration of database queries.",
            ["tool"],
            registry=self.registry,
        )
        self.jwks_refresh = Counter(
            "iot_mcp_bridge_jwks_refresh_total",
            "JWKS cache refreshes partitioned by result (ok|error).",
            ["result"],
            registry=self.registry,
        )


_metrics: Metrics | None = None


def init() -> Metrics:
    """Create the module-level metrics singleton (idempotent)."""
    global _metrics
    if _metrics is None:
        _metrics = Metrics()
    return _metrics


def get() -> Metrics:
    """Return the module-level metrics singleton; lazily creates one if absent."""
    if _metrics is None:
        return init()
    return _metrics


def reset() -> None:
    """Drop the singleton — for tests that need a fresh registry."""
    global _metrics
    _metrics = None


async def serve(metrics: Metrics, port: int) -> asyncio.AbstractServer:
    """Start a tiny HTTP server exposing /metrics on the given port."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                writer.close()
                return
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break

            parts = request_line.decode("ascii", errors="replace").split()
            path = parts[1] if len(parts) >= 2 else "/"

            if path.startswith("/metrics"):
                body = generate_latest(metrics.registry)
                writer.write(
                    b"HTTP/1.1 200 OK\r\n"
                    + f"Content-Type: {CONTENT_TYPE_LATEST}\r\n".encode("ascii")
                    + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                    + body
                )
            else:
                body = b"not found\n"
                writer.write(
                    b"HTTP/1.1 404 Not Found\r\n"
                    b"Content-Type: text/plain\r\n"
                    + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                    + body
                )
            await writer.drain()
        except Exception:
            logger.exception("metrics http handler failed")
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    server = await asyncio.start_server(handle, host="0.0.0.0", port=port)  # noqa: S104
    logger.info("metrics server listening on :%d", port)
    return server
