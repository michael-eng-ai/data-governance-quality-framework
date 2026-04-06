"""Application configuration via environment variables.

Why pydantic-settings: provides type-safe, validated configuration that fails
fast on missing required settings rather than silently using defaults that
could cause production issues.
"""

from __future__ import annotations

import logging
import sys
from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pythonjsonlogger import jsonlogger


class Settings(BaseSettings):
    """Centralized application settings loaded from environment variables.

    Why a single settings class: avoids scattered os.getenv calls that are
    easy to miss during configuration audits and hard to test.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Database
    database_url: str = Field(
        ...,
        description="PostgreSQL connection string",
    )
    database_pool_size: int = Field(default=10, ge=1, le=100)
    database_max_overflow: int = Field(default=20, ge=0, le=100)

    # Application
    app_env: Literal["development", "staging", "production", "test"] = "development"
    app_debug: bool = False
    app_log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    app_host: str = "0.0.0.0"
    app_port: int = Field(default=8000, ge=1, le=65535)

    # Quality Engine
    quality_check_timeout_seconds: int = Field(default=300, ge=10, le=3600)
    quality_max_concurrent_checks: int = Field(default=5, ge=1, le=50)

    # Alerting
    alert_webhook_url: str | None = None
    alert_email_smtp_host: str | None = None
    alert_email_smtp_port: int = 587
    alert_email_from: str | None = None
    alert_email_to: str | None = None

    # Contracts
    contracts_directory: str = "./contracts"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings singleton.

    Why lru_cache: avoids re-parsing environment variables on every call
    while still allowing test overrides via cache_clear().
    """
    return Settings()


def configure_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure structured JSON logging for production observability.

    Why JSON logging: enables structured log ingestion by systems like
    ELK, Datadog, and CloudWatch without custom parsers.
    """
    logger = logging.getLogger("data_governance")
    logger.setLevel(getattr(logging, log_level.upper()))

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
