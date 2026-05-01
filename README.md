# iot-mcp-bridge

A read-only [Model Context Protocol](https://modelcontextprotocol.io) server that lets a large language model — Claude.ai, a local Ollama instance, or any other MCP-aware client — answer questions about an **IoT-enabled home**: heating, photovoltaics, EV charging, KNX bus events, room climate.

Instead of giving the LLM raw SQL access (brittle, hard to bound, no audit), `iot-mcp-bridge` exposes a small set of well-shaped tools backed by **TimescaleDB hypertables** plus their **continuous aggregates**. The LLM picks a tool, the server runs a parametrised query against a long-term timeseries store, and the result comes back already aggregated and capped to a token-friendly size.

## Why

Modern homes generate a lot of telemetry — KNX writes, smart-meter readings, heat-pump flow temperatures, wallbox sessions, inverter data — and most of it lands in some database that nobody ever queries. Pointing an LLM at the database directly works for prototypes but breaks down quickly: the model invents column names, returns 50,000 rows, can't tell hypertables apart from rollups, and there is no policy on what it's allowed to read.

`iot-mcp-bridge` is the small, opinionated middle layer:

- **Discoverable** — the LLM can list data sources and inspect schemas, including a sample of JSONB keys for raw payloads.
- **Aggregation-aware** — when a query asks for hourly buckets or coarser, the server transparently routes to a TimescaleDB continuous aggregate, returning fewer rows and faster responses.
- **Read-only by construction** — the server only ever connects with a Postgres role that has `SELECT` privileges. There is no `execute_sql` tool.
- **Result-bounded** — every tool caps its output. The LLM cannot accidentally pull a year of 5-second sensor data into its context window.
- **Pluggable** — the data source is just Postgres + TimescaleDB. There is nothing homelab-specific in the server itself; the schema discovery works on any TimescaleDB instance.

## What it gives you

In Phase 0 (foundation), the server exposes three tools:

| Tool                                                                             | What it does                                                                                                                                                                            |
| -------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `list_data_sources()`                                                            | Enumerates all hypertables and continuous aggregates in the connected database, with their time range.                                                                                  |
| `get_schema(table)`                                                              | Returns columns, types, and — for tables with a `JSONB` payload column — a sample of the most common JSON keys, so the LLM can construct sensible expressions like `raw->>'flow_temp'`. |
| `query_timeseries(table, columns, from_ts, to_ts, aggregation, bucket, filters)` | Bucketed aggregate query (`avg`, `sum`, `min`, `max`, `count`). Automatically uses a `*_1h` continuous aggregate when the bucket is one hour or coarser and an aggregate exists.        |

Later phases (tracked separately) add domain tools, anomaly detection, live state via NATS, optimization advisors, and approval-gated control. Each phase is fully optional — you can run Phase 0 forever and still get a useful "ask the house" experience.

## Standalone usage

`iot-mcp-bridge` does not depend on any specific cluster, ingress, or auth setup. You can run it against any TimescaleDB instance you already have.

### Prerequisites

- Python 3.14
- [uv](https://docs.astral.sh/uv/)
- A Postgres database with the [TimescaleDB](https://www.timescale.com/) extension and at least one hypertable
- A Postgres role with `SELECT` on the schemas/tables you want to expose

### Run it

```bash
git clone https://github.com/alexander-zimmermann/iot-mcp-bridge.git
cd iot-mcp-bridge
uv sync

export MCP_DB_HOST=localhost
export MCP_DB_PORT=5432
export MCP_DB_NAME=mydb
export MCP_DB_USER=mcp_readonly
export MCP_DB_PASSWORD=secret

uv run iot-mcp-bridge
# server listening on http://0.0.0.0:8080/mcp
```

### Connect Claude.ai

Open Claude.ai → Settings → Connectors → "Add custom connector" → URL `http://localhost:8080/mcp` (or wherever you exposed the server). Start a chat and ask "what data sources do you have?" — the connector will surface the three tools.

For a production deployment, put the server behind TLS and an authenticating proxy (or enable the built-in Authentik OAuth + JWKS validation, see configuration below).

### Configuration

All settings are environment variables, prefixed with `MCP_`:

| Variable              | Default    | Purpose                                         |
| --------------------- | ---------- | ----------------------------------------------- |
| `MCP_HOST`            | `0.0.0.0`  | Bind address                                    |
| `MCP_PORT`            | `8080`     | Bind port                                       |
| `MCP_LOG_LEVEL`       | `INFO`     | structlog level                                 |
| `MCP_LOG_FORMAT`      | `json`     | `json` or `console`                             |
| `MCP_DB_HOST`         | _required_ | Postgres host                                   |
| `MCP_DB_PORT`         | `5432`     | Postgres port                                   |
| `MCP_DB_NAME`         | _required_ | Database name                                   |
| `MCP_DB_USER`         | _required_ | Read-only role                                  |
| `MCP_DB_PASSWORD`     | _required_ | Password                                        |
| `MCP_DB_POOL_MIN`     | `2`        | psycopg pool min size                           |
| `MCP_DB_POOL_MAX`     | `10`       | psycopg pool max size                           |
| `MCP_QUERY_ROW_LIMIT` | `5000`     | Hard cap on rows returned by `query_timeseries` |
| `MCP_AUTH_ENABLED`    | `false`    | Enable Bearer-token validation                  |
| `MCP_AUTH_JWKS_URL`   | —          | JWKS endpoint when auth is enabled              |
| `MCP_AUTH_ISSUER`     | —          | Expected `iss` claim                            |
| `MCP_AUTH_AUDIENCE`   | —          | Expected `aud` claim                            |

When `MCP_AUTH_ENABLED=true`, every request must carry a valid OIDC Bearer token signed by the configured JWKS. Tested against [Authentik](https://goauthentik.io/) but works with any OIDC-compliant authorization server.

### Container image

Multi-arch images are published on every release tag:

```bash
docker run --rm -p 8080:8080 \
  -e MCP_DB_HOST=host.docker.internal \
  -e MCP_DB_NAME=mydb \
  -e MCP_DB_USER=mcp_readonly \
  -e MCP_DB_PASSWORD=secret \
  ghcr.io/alexander-zimmermann/iot-mcp-bridge:latest
```

## Project status

Phase 0a (foundation) — in development. Tracked as [homelab#749](https://github.com/alexander-zimmermann/homelab/issues/749). Subsequent phases are listed at the top of the [homelab repository's iot-mcp-bridge issue label](https://github.com/alexander-zimmermann/homelab/labels/iot-mcp-bridge).

## Development

```bash
uv sync --extra dev
uv run ruff check .
uv run mypy src
uv run pytest -q
```

Tests use [`testcontainers`](https://testcontainers-python.readthedocs.io/) to spin up a real TimescaleDB instance — no DB mocks.

## License

GPL-2.0-or-later. See [LICENSE](LICENSE).
