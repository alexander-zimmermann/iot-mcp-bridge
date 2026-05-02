"""Auth module tests: JWKS roundtrip, claim validation, ASGI middleware."""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import httpx
import jwt
import pytest
import respx
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from jwt.algorithms import RSAAlgorithm
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from iot_mcp_bridge import auth as auth_module
from iot_mcp_bridge.auth import AuthError, AuthMiddleware, Principal, verify_token
from iot_mcp_bridge.config import Settings

JWKS_URL = "https://issuer.test/jwks"
ISSUER = "https://issuer.test/"
AUDIENCE = "iot-mcp-bridge"
RESOURCE_URL = "https://mcp.test/mcp"


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "db_host": "localhost",
        "db_name": "homelab",
        "db_username": "x",
        "db_password": "x",
        "auth_enabled": True,
        "auth_jwks_url": JWKS_URL,
        "auth_issuer": ISSUER,
        "auth_audience": AUDIENCE,
        "auth_resource_url": RESOURCE_URL,
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def _make_keypair() -> RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _public_jwk(private_key: RSAPrivateKey, kid: str) -> dict[str, Any]:
    jwk: dict[str, Any] = json.loads(RSAAlgorithm.to_jwk(private_key.public_key()))
    jwk.update({"kid": kid, "alg": "RS256", "use": "sig"})
    return jwk


def _sign(private_key: RSAPrivateKey, kid: str, claims: dict[str, Any]) -> str:
    return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": kid})


def _claims(**overrides: Any) -> dict[str, Any]:
    now = int(time.time())
    base: dict[str, Any] = {
        "sub": "user-123",
        "iss": ISSUER,
        "aud": AUDIENCE,
        "iat": now - 5,
        "exp": now + 60,
        "azp": AUDIENCE,
    }
    base.update(overrides)
    return base


@pytest.fixture
def keypair() -> RSAPrivateKey:
    return _make_keypair()


@pytest.fixture
def jwks_mock(keypair: RSAPrivateKey) -> Iterator[respx.MockRouter]:
    jwk = _public_jwk(keypair, "key1")
    with respx.mock(assert_all_called=False) as router:
        router.get(JWKS_URL).mock(return_value=httpx.Response(200, json={"keys": [jwk]}))
        yield router


@pytest.fixture
def configured(jwks_mock: respx.MockRouter) -> Iterator[Settings]:
    settings = _settings()
    auth_module.configure(settings)
    try:
        yield settings
    finally:
        auth_module.configure(
            Settings(
                db_host="localhost",
                db_name="x",
                db_username="x",
                db_password="x",
                auth_enabled=False,
            )
        )


# ------------- Settings validation (sync) -------------


def test_disabled_auth_skips_validation() -> None:
    s = Settings(
        db_host="localhost",
        db_name="x",
        db_username="x",
        db_password="x",
        auth_enabled=False,
    )
    assert s.auth_enabled is False


def test_enabled_auth_requires_jwks_config() -> None:
    with pytest.raises(ValueError, match="MCP_AUTH_JWKS_URL"):
        Settings(
            db_host="localhost",
            db_name="x",
            db_username="x",
            db_password="x",
            auth_enabled=True,
            auth_issuer="x",
            auth_audience="x",
        )


# ------------- verify_token -------------


@pytest.mark.asyncio
async def test_disabled_auth_passes_through() -> None:
    s = Settings(
        db_host="localhost",
        db_name="x",
        db_username="x",
        db_password="x",
        auth_enabled=False,
    )
    auth_module.configure(s)
    assert await verify_token(None, s) is None
    assert await verify_token("anything", s) is None


@pytest.mark.asyncio
async def test_missing_token_raises(configured: Settings) -> None:
    with pytest.raises(AuthError, match="missing_bearer_token"):
        await verify_token(None, configured)


@pytest.mark.asyncio
async def test_valid_token_returns_principal(configured: Settings, keypair: RSAPrivateKey) -> None:
    token = _sign(keypair, "key1", _claims(sub="alex", azp="iot-mcp-bridge"))
    principal = await verify_token(token, configured)
    assert isinstance(principal, Principal)
    assert principal.sub == "alex"
    assert principal.client_id == "iot-mcp-bridge"
    assert principal.claims["iss"] == ISSUER


@pytest.mark.asyncio
async def test_expired_token_raises(configured: Settings, keypair: RSAPrivateKey) -> None:
    now = int(time.time())
    token = _sign(keypair, "key1", _claims(iat=now - 3600, exp=now - 60))
    with pytest.raises(AuthError, match="token_expired"):
        await verify_token(token, configured)


@pytest.mark.asyncio
async def test_wrong_audience_raises(configured: Settings, keypair: RSAPrivateKey) -> None:
    token = _sign(keypair, "key1", _claims(aud="wrong-audience"))
    with pytest.raises(AuthError, match="invalid_audience"):
        await verify_token(token, configured)


@pytest.mark.asyncio
async def test_wrong_issuer_raises(configured: Settings, keypair: RSAPrivateKey) -> None:
    token = _sign(keypair, "key1", _claims(iss="https://attacker.test/"))
    with pytest.raises(AuthError, match="invalid_issuer"):
        await verify_token(token, configured)


@pytest.mark.asyncio
async def test_signature_from_unknown_key_raises(
    configured: Settings,
) -> None:
    # Token kid is not in JWKS even after a refresh.
    foreign = _make_keypair()
    token = _sign(foreign, "key-unknown", _claims())
    with pytest.raises(AuthError, match="unknown_signing_key"):
        await verify_token(token, configured)


@pytest.mark.asyncio
async def test_jwks_refreshes_on_unknown_kid(keypair: RSAPrivateKey) -> None:
    """A token signed with kid=key2 forces a JWKS refresh that returns key2."""
    jwk_v1 = _public_jwk(keypair, "key1")
    jwk_v2 = _public_jwk(keypair, "key2")

    settings = _settings()
    with respx.mock(assert_all_called=False) as router:
        route = router.get(JWKS_URL).mock(
            side_effect=[
                httpx.Response(200, json={"keys": [jwk_v1]}),
                httpx.Response(200, json={"keys": [jwk_v1, jwk_v2]}),
            ]
        )
        auth_module.configure(settings)

        # Prime cache with kid=key1 (1 fetch).
        token1 = _sign(keypair, "key1", _claims())
        assert await verify_token(token1, settings) is not None

        # Sign with kid=key2 — cache miss must trigger one more fetch.
        token2 = _sign(keypair, "key2", _claims())
        principal = await verify_token(token2, settings)
        assert principal is not None
        assert route.call_count == 2


# ------------- AuthMiddleware -------------


async def _ok(request: Request) -> PlainTextResponse:
    return PlainTextResponse(f"hello {request.scope.get('path')}")


def _build_app(settings: Settings) -> AuthMiddleware:
    inner = Starlette(
        routes=[
            Route("/mcp", _ok),
            Route("/healthz", _ok),
            Route("/.well-known/oauth-protected-resource", _ok),
        ]
    )
    return AuthMiddleware(inner, settings)


@pytest.mark.asyncio
async def test_middleware_401_without_token(configured: Settings) -> None:
    app = _build_app(configured)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://test") as c:
        r = await c.get("/mcp")
    assert r.status_code == 401
    challenge = r.headers["www-authenticate"]
    assert 'realm="iot-mcp-bridge"' in challenge
    assert 'error="invalid_token"' in challenge
    assert "resource_metadata=" in challenge


@pytest.mark.asyncio
async def test_middleware_401_on_expired_token(
    configured: Settings, keypair: RSAPrivateKey
) -> None:
    now = int(time.time())
    token = _sign(keypair, "key1", _claims(iat=now - 3600, exp=now - 60))
    app = _build_app(configured)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://test") as c:
        r = await c.get("/mcp", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 401
    assert "token_expired" in r.headers["www-authenticate"]


@pytest.mark.asyncio
async def test_middleware_200_with_valid_token(
    configured: Settings, keypair: RSAPrivateKey
) -> None:
    token = _sign(keypair, "key1", _claims())
    app = _build_app(configured)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://test") as c:
        r = await c.get("/mcp", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    assert r.text == "hello /mcp"


@pytest.mark.asyncio
async def test_middleware_healthz_bypasses_auth(configured: Settings) -> None:
    app = _build_app(configured)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://test") as c:
        r = await c.get("/healthz")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_middleware_well_known_bypasses_auth(configured: Settings) -> None:
    app = _build_app(configured)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://test") as c:
        r = await c.get("/.well-known/oauth-protected-resource")
    assert r.status_code == 200
