"""Bearer-token validation against an OIDC JWKS endpoint.

Activated when ``MCP_AUTH_ENABLED=true``. The MCP endpoint then requires a
valid JWT issued by ``MCP_AUTH_ISSUER`` (Authentik in production), signed by
a key from ``MCP_AUTH_JWKS_URL``, and bound to ``MCP_AUTH_AUDIENCE``.

The transport hook is :class:`AuthMiddleware` (pure ASGI). On success it
binds ``sub`` and ``client_id`` to structlog's contextvars so every log
line emitted while handling the request carries the caller identity.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit

import httpx
import jwt
import structlog
from jwt import PyJWK, PyJWKSet

from .config import Settings
from .logging import get_logger

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

log = get_logger(__name__)


@dataclass(frozen=True)
class Principal:
    sub: str
    client_id: str | None
    claims: dict[str, object]


class AuthError(Exception):
    """Raised when a token is invalid; ``reason`` is logged and surfaced via 401."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class JwksCache:
    """JWKS fetched lazily over httpx, cached with a TTL.

    httpx is used (not urllib via PyJWKClient) so respx can intercept the
    request in tests and the production path stays fully async.
    """

    def __init__(self, url: str, ttl_seconds: int) -> None:
        self._url = url
        self._ttl = ttl_seconds
        self._jwks: PyJWKSet | None = None
        self._loaded_at: float = 0.0

    async def _refresh(self) -> None:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(self._url)
            resp.raise_for_status()
            self._jwks = PyJWKSet.from_dict(resp.json())
            self._loaded_at = time.time()
            log.info("jwks_refreshed", url=self._url, key_count=len(self._jwks.keys))

    async def get_signing_key(self, kid: str | None) -> PyJWK:
        if self._jwks is None or (time.time() - self._loaded_at) > self._ttl:
            await self._refresh()
        assert self._jwks is not None
        try:
            return self._lookup(kid)
        except KeyError:
            # Unknown kid — issuer may have rotated keys; refresh once.
            await self._refresh()
            return self._lookup(kid)

    def _lookup(self, kid: str | None) -> PyJWK:
        assert self._jwks is not None
        if kid is None:
            keys = list(self._jwks.keys)
            if len(keys) != 1:
                raise KeyError("token_missing_kid")
            return keys[0]
        for key in self._jwks.keys:
            if key.key_id == kid:
                return key
        raise KeyError(kid)

    def invalidate(self) -> None:
        self._jwks = None
        self._loaded_at = 0.0


_jwks: JwksCache | None = None


def configure(settings: Settings) -> None:
    """Initialize (or tear down) the module-level JWKS cache."""
    global _jwks
    if settings.auth_enabled and settings.auth_jwks_url:
        _jwks = JwksCache(settings.auth_jwks_url, settings.auth_jwks_ttl_seconds)
    else:
        _jwks = None


async def verify_token(token: str | None, settings: Settings) -> Principal | None:
    if not settings.auth_enabled:
        return None
    if not token:
        raise AuthError("missing_bearer_token")
    if _jwks is None:
        raise AuthError("auth_not_configured")

    try:
        unverified_header = jwt.get_unverified_header(token)
    except jwt.InvalidTokenError as exc:
        raise AuthError("invalid_token") from exc

    kid = unverified_header.get("kid")
    try:
        signing_key = await _jwks.get_signing_key(kid)
    except KeyError as exc:
        raise AuthError("unknown_signing_key") from exc

    try:
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=settings.auth_audience,
            issuer=settings.auth_issuer,
            options={"require": ["exp", "iat", "iss", "aud"]},
        )
    except jwt.ExpiredSignatureError as exc:
        raise AuthError("token_expired") from exc
    except jwt.InvalidAudienceError as exc:
        raise AuthError("invalid_audience") from exc
    except jwt.InvalidIssuerError as exc:
        raise AuthError("invalid_issuer") from exc
    except jwt.InvalidTokenError as exc:
        raise AuthError("invalid_token") from exc

    sub = str(claims.get("sub") or "")
    if not sub:
        raise AuthError("missing_sub_claim")

    cid = claims.get("azp") or claims.get("client_id")
    return Principal(
        sub=sub,
        client_id=str(cid) if cid is not None else None,
        claims=claims,
    )


def _bearer_from_headers(headers: list[tuple[bytes, bytes]]) -> str | None:
    for name, value in headers:
        if name.lower() == b"authorization":
            decoded = value.decode("latin-1")
            if decoded[:7].lower() == "bearer ":
                token = decoded[7:].strip()
                return token or None
            return None
    return None


def _resource_metadata_url(settings: Settings) -> str | None:
    if not settings.auth_resource_url:
        return None
    parts = urlsplit(settings.auth_resource_url)
    return urlunsplit((parts.scheme, parts.netloc, "/.well-known/oauth-protected-resource", "", ""))


def _www_authenticate(reason: str, settings: Settings) -> str:
    challenge = (
        f'Bearer realm="iot-mcp-bridge", error="invalid_token", error_description="{reason}"'
    )
    metadata_url = _resource_metadata_url(settings)
    if metadata_url:
        challenge += f', resource_metadata="{metadata_url}"'
    return challenge


async def _send_unauthorized(send: Send, reason: str, settings: Settings) -> None:
    body = b'{"error":"unauthorized"}'
    headers = [
        (b"content-type", b"application/json"),
        (b"www-authenticate", _www_authenticate(reason, settings).encode("ascii")),
        (b"content-length", str(len(body)).encode("ascii")),
    ]
    await send({"type": "http.response.start", "status": 401, "headers": headers})
    await send({"type": "http.response.body", "body": body})


def oauth_protected_resource_metadata(settings: Settings) -> dict[str, object]:
    """RFC 9728 protected-resource metadata document."""
    return {
        "resource": settings.auth_resource_url or "",
        "authorization_servers": ([settings.auth_issuer] if settings.auth_issuer else []),
        "bearer_methods_supported": ["header"],
    }


# Paths that bypass authentication: liveness probe and OAuth discovery.
_PUBLIC_PATHS: frozenset[str] = frozenset({"/healthz"})
_PUBLIC_PREFIXES: tuple[str, ...] = ("/.well-known/",)


def _is_public_path(path: str) -> bool:
    return path in _PUBLIC_PATHS or any(path.startswith(p) for p in _PUBLIC_PREFIXES)


class AuthMiddleware:
    """Pure ASGI middleware that gates HTTP requests on a valid Bearer JWT.

    When auth is disabled or the path is public, requests pass straight
    through. Otherwise the token is validated and the resulting
    :class:`Principal` is bound into structlog contextvars (``sub``,
    ``client_id``) for the duration of the request.
    """

    def __init__(self, app: ASGIApp, settings: Settings) -> None:
        self.app = app
        self.settings = settings

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not self.settings.auth_enabled:
            await self.app(scope, receive, send)
            return

        path = str(scope.get("path", ""))
        if _is_public_path(path):
            await self.app(scope, receive, send)
            return

        token = _bearer_from_headers(scope.get("headers", []))
        try:
            principal = await verify_token(token, self.settings)
        except AuthError as exc:
            log.info(
                "auth_rejected",
                reason=exc.reason,
                path=path,
                method=scope.get("method"),
            )
            await _send_unauthorized(send, exc.reason, self.settings)
            return

        if principal is None:
            await self.app(scope, receive, send)
            return

        bound = structlog.contextvars.bind_contextvars(
            sub=principal.sub,
            client_id=principal.client_id,
        )
        log.info(
            "auth_accepted",
            path=path,
            method=scope.get("method"),
        )
        try:
            await self.app(scope, receive, send)
        finally:
            structlog.contextvars.unbind_contextvars(*bound.keys())


# Help mypy without forcing a runtime import — referenced only for typing above.
if TYPE_CHECKING:
    _ = Message
