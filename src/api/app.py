"""FastAPI application factory.

Why a factory pattern: enables creating the app with different configurations
for production, testing, and development. Test fixtures can override settings
without polluting the global app state.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes.contracts import router as contracts_router
from src.api.routes.governance import router as governance_router
from src.api.routes.health import router as health_router
from src.api.routes.quality import router as quality_router
from src.config import configure_logging, get_settings

logger = logging.getLogger("data_governance")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifecycle manager.

    Why lifespan context: replaces deprecated on_event handlers and
    ensures cleanup runs even on ungraceful shutdowns.
    """
    settings = get_settings()
    configure_logging(settings.app_log_level)

    logger.info(
        "application_starting",
        extra={
            "event": "application_starting",
            "environment": settings.app_env,
            "log_level": settings.app_log_level,
        },
    )

    yield

    logger.info("application_shutting_down", extra={"event": "application_shutting_down"})


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns:
        Configured FastAPI instance with all routes and middleware.
    """
    settings = get_settings()

    app = FastAPI(
        title="Data Governance & Quality Framework",
        description=(
            "Automated data quality gates with contract enforcement, "
            "freshness SLAs, and governance metrics."
        ),
        version="1.0.0",
        docs_url="/docs" if settings.app_env != "production" else None,
        redoc_url="/redoc" if settings.app_env != "production" else None,
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.app_env == "development" else [],
        allow_credentials=True,
        allow_methods=["GET", "POST", "DELETE"],
        allow_headers=["*"],
    )

    app.include_router(health_router, tags=["health"])
    app.include_router(contracts_router, prefix="/contracts", tags=["contracts"])
    app.include_router(quality_router, prefix="/quality", tags=["quality"])
    app.include_router(governance_router, prefix="/governance", tags=["governance"])

    return app
