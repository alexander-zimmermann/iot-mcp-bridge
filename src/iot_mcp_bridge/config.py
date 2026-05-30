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

    # Separate write credentials for the batch-jobs image. Optional so the
    # read-only server keeps validating without them; the jobs CLI calls
    # `require_db_write()` before opening a writer connection.
    db_write_username: str = ""
    db_write_password: str = Field(default="", repr=False)
    db_write_username_file: str | None = None
    db_write_password_file: str | None = None

    # NATS — jobs publish anomaly events on `anomaly.<uc>.<severity>`.
    nats_servers: str | None = None
    nats_user: str | None = None
    nats_password: str = Field(default="", repr=False)
    nats_password_file: str | None = None
    nats_creds_file: str | None = None

    # S3 (rustfs) — jobs persist trained models as joblib pickles.
    s3_endpoint: str | None = None
    s3_access_key: str = ""
    s3_secret_key: str = Field(default="", repr=False)
    s3_access_key_file: str | None = None
    s3_secret_key_file: str | None = None
    s3_bucket: str = "iot-mcp-bridge-models"
    s3_region: str = "us-east-1"

    # SMTP — weekly-report job sends Markdown via the cluster-internal relay
    # (no auth, BGP-advertised LoadBalancer).
    smtp_host: str = "smtprelay.smtprelay.svc.cluster.local"
    smtp_port: int = 25
    smtp_from: str = "admin@zimmermann.sh"
    smtp_to: str = "admin@zimmermann.sh"

    query_row_limit: int = 5000

    metrics_port: int = 9090

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
    def _resolve_optional_secret_files(self) -> Settings:
        # Resolve *_file fields lazily. Empty values stay empty — the jobs CLI
        # validates the specific subset it actually needs at runtime.
        if self.db_write_username_file:
            self.db_write_username = (
                Path(self.db_write_username_file).read_text(encoding="utf-8").strip()
            )
        if self.db_write_password_file:
            self.db_write_password = (
                Path(self.db_write_password_file).read_text(encoding="utf-8").strip()
            )
        if self.nats_password_file:
            self.nats_password = (
                Path(self.nats_password_file).read_text(encoding="utf-8").strip()
            )
        if self.s3_access_key_file:
            self.s3_access_key = (
                Path(self.s3_access_key_file).read_text(encoding="utf-8").strip()
            )
        if self.s3_secret_key_file:
            self.s3_secret_key = (
                Path(self.s3_secret_key_file).read_text(encoding="utf-8").strip()
            )
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

    @property
    def db_write_dsn(self) -> str:
        # Raises ValueError when called without write creds set — fail loud
        # rather than silently falling back to the read-only DSN.
        if not self.db_write_username or not self.db_write_password:
            raise ValueError(
                "MCP_DB_WRITE_USERNAME / MCP_DB_WRITE_PASSWORD "
                "(or *_FILE variants) are required for the jobs image"
            )
        return (
            f"postgresql://{self.db_write_username}:{self.db_write_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


def load_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
