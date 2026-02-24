"""
config.py — pydantic-settings Settings class.

All environment variables for the candata platform are declared here.
Both the pipeline and API import `settings` from this module.

Usage:
    from candata_shared.config import settings
    print(settings.supabase_url)
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_dotenv() -> Path | None:
    """Walk up from CWD to find the nearest .env file."""
    current = Path.cwd()
    for parent in [current, *current.parents]:
        candidate = parent / ".env"
        if candidate.is_file():
            return candidate
    return None


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_find_dotenv() or ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # -------------------------------------------------------------------------
    # Supabase
    # -------------------------------------------------------------------------
    supabase_url: str = Field(default="http://localhost:54321")
    supabase_anon_key: str = Field(default="")
    supabase_service_key: str = Field(default="")
    database_url: str = Field(
        default="postgresql://postgres:postgres@localhost:54322/postgres"
    )

    # -------------------------------------------------------------------------
    # DuckDB
    # -------------------------------------------------------------------------
    duckdb_path: str = Field(default="./data/staging.duckdb")

    # -------------------------------------------------------------------------
    # Data sources
    # -------------------------------------------------------------------------
    statcan_base_url: str = Field(
        default="https://www150.statcan.gc.ca"
    )
    boc_valet_url: str = Field(
        default="https://www.bankofcanada.ca/valet"
    )
    cmhc_base_url: str = Field(
        default="https://api.cmhc-schl.gc.ca/housingObserver"
    )
    buyandsell_base_url: str = Field(
        default="https://canadabuys.canada.ca/en/tender-opportunities/api/v1"
    )

    # -------------------------------------------------------------------------
    # API server
    # -------------------------------------------------------------------------
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    cors_origins: str = Field(default="http://localhost:3000,http://localhost:3001")
    jwt_secret: str = Field(default="change-me-in-production")

    # Rate limits (requests per minute per tier)
    rate_limit_free: int = Field(default=60)
    rate_limit_starter: int = Field(default=300)
    rate_limit_pro: int = Field(default=1000)
    rate_limit_business: int = Field(default=5000)

    # -------------------------------------------------------------------------
    # Stripe
    # -------------------------------------------------------------------------
    stripe_secret_key: str = Field(default="")
    stripe_publishable_key: str = Field(default="")
    stripe_webhook_secret: str = Field(default="")
    stripe_price_starter: str = Field(default="")
    stripe_price_pro: str = Field(default="")
    stripe_price_business: str = Field(default="")

    # -------------------------------------------------------------------------
    # Logging
    # -------------------------------------------------------------------------
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO"
    )
    log_format: Literal["json", "console"] = Field(default="console")

    # -------------------------------------------------------------------------
    # Derived / computed
    # -------------------------------------------------------------------------
    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @field_validator("supabase_url", "statcan_base_url", "boc_valet_url", mode="before")
    @classmethod
    def strip_trailing_slash(cls, v: str) -> str:
        return v.rstrip("/") if isinstance(v, str) else v


# ---------------------------------------------------------------------------
# Module-level singleton — import this everywhere
# ---------------------------------------------------------------------------
settings = Settings()
