"""Bearer-token validation against an OIDC JWKS endpoint.

Phase 0a runs with `MCP_AUTH_ENABLED=false` — `verify_token` short-circuits and
returns ``None``. Phase 0b activates the flag and exercises the validation path.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import httpx
import jwt
from jwt import PyJWKClient

from .config import Settings
from .logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class Principal:
    sub: str
    client_id: str | None
    claims: dict[str, object]


class AuthError(Exception):
    """Raised when a token is invalid; carries an HTTP-friendly message."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _JwksCache:
    def __init__(self, url: str, ttl_seconds: int) -> None:
        self._url = url
        self._ttl = ttl_seconds
        self._client: PyJWKClient | None = None
        self._loaded_at: float = 0.0

    def get(self) -> PyJWKClient:
        now = time.time()
        if self._client is None or (now - self._loaded_at) > self._ttl:
            log.info("jwks_refresh", url=self._url)
            self._client = PyJWKClient(self._url, cache_keys=True)
            self._loaded_at = now
        return self._client

    def invalidate(self) -> None:
        self._client = None
        self._loaded_at = 0.0


_jwks: _JwksCache | None = None


def configure(settings: Settings) -> None:
    global _jwks
    if settings.auth_enabled and settings.auth_jwks_url:
        _jwks = _JwksCache(settings.auth_jwks_url, settings.auth_jwks_ttl_seconds)


def verify_token(token: str | None, settings: Settings) -> Principal | None:
    if not settings.auth_enabled:
        return None
    if not token:
        raise AuthError("missing_bearer_token")
    if _jwks is None:
        raise AuthError("auth_not_configured")

    try:
        signing_key = _jwks.get().get_signing_key_from_jwt(token).key
    except jwt.exceptions.PyJWKClientError:
        _jwks.invalidate()
        try:
            signing_key = _jwks.get().get_signing_key_from_jwt(token).key
        except jwt.exceptions.PyJWKClientError as exc:
            raise AuthError("unknown_signing_key") from exc

    try:
        claims = jwt.decode(
            token,
            signing_key,
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

    sub = str(claims.get("sub", ""))
    if not sub:
        raise AuthError("missing_sub_claim")

    cid = claims.get("azp") or claims.get("client_id")
    return Principal(
        sub=sub,
        client_id=str(cid) if cid is not None else None,
        claims=claims,
    )


async def fetch_oidc_metadata(issuer: str) -> dict[str, object]:
    """Fetch `.well-known/openid-configuration` from the issuer (used in tests)."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        url = issuer.rstrip("/") + "/.well-known/openid-configuration"
        resp = await client.get(url)
        resp.raise_for_status()
        data: dict[str, object] = resp.json()
        return data
