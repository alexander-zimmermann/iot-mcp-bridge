from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MCP_", env_file=".env", extra="ignore")

    host: str = "0.0.0.0"  # noqa: S104
    port: int = 8080
    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "json"

    db_host: str
    db_port: int = 5432
    db_name: str
    # In-cluster the credentials come from a mounted Secret via *_file paths;
    # for tests/local dev the literal env vars still work.
    db_username: str = ""
    db_password: str = Field(default="", repr=False)
    db_username_file: str | None = None
    db_password_file: str | None = None
    db_pool_min: int = 2
    db_pool_max: int = 10

    query_row_limit: int = 5000

    auth_enabled: bool = False
    auth_jwks_url: str | None = None
    auth_issuer: str | None = None
    auth_audience: str | None = None
    auth_jwks_ttl_seconds: int = 3600
    auth_resource_url: str | None = None

    @model_validator(mode="after")
    def _resolve_db_secret_files(self) -> Settings:
        if self.db_username_file:
            self.db_username = Path(self.db_username_file).read_text(encoding="utf-8").strip()
        if self.db_password_file:
            self.db_password = Path(self.db_password_file).read_text(encoding="utf-8").strip()
        if not self.db_username:
            raise ValueError("MCP_DB_USERNAME or MCP_DB_USERNAME_FILE is required")
        if not self.db_password:
            raise ValueError("MCP_DB_PASSWORD or MCP_DB_PASSWORD_FILE is required")
        return self

    @model_validator(mode="after")
    def _check_auth_config(self) -> Settings:
        if self.auth_enabled:
            missing = [
                name
                for name, value in (
                    ("MCP_AUTH_JWKS_URL", self.auth_jwks_url),
                    ("MCP_AUTH_ISSUER", self.auth_issuer),
                    ("MCP_AUTH_AUDIENCE", self.auth_audience),
                )
                if not value
            ]
            if missing:
                raise ValueError(f"MCP_AUTH_ENABLED=true requires {', '.join(missing)}")
        return self

    @property
    def db_dsn(self) -> str:
        return (
            f"postgresql://{self.db_username}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


def load_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
