"""Auth module tests.

In Phase 0a the auth flag is off; we exercise the no-op path here. Phase 0b
will replace these with full JWKS roundtrip tests using a mock issuer.
"""

from __future__ import annotations

import pytest

from iot_mcp_bridge.auth import AuthError, verify_token
from iot_mcp_bridge.config import Settings


def _settings(**overrides: object) -> Settings:
    base = {
        "db_host": "localhost",
        "db_name": "homelab",
        "db_user": "x",
        "db_password": "x",
        "auth_enabled": False,
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def test_disabled_auth_passes_through() -> None:
    s = _settings(auth_enabled=False)
    assert verify_token(None, s) is None
    assert verify_token("anything", s) is None


def test_enabled_auth_without_token_raises() -> None:
    s = _settings(
        auth_enabled=True,
        auth_jwks_url="https://example.test/jwks",
        auth_issuer="https://example.test/",
        auth_audience="iot-mcp-bridge",
    )
    with pytest.raises(AuthError, match="missing_bearer_token"):
        verify_token(None, s)


def test_enabled_auth_requires_jwks_config() -> None:
    with pytest.raises(ValueError, match="MCP_AUTH_JWKS_URL"):
        _settings(auth_enabled=True, auth_issuer="x", auth_audience="x")
