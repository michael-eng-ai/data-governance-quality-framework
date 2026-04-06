"""Database session management.

Why SQLAlchemy engine with connection pooling: provides connection reuse,
automatic health checks, and graceful handling of connection failures.
Using raw connections instead of ORM keeps the overhead minimal for a
framework that primarily runs parameterized queries.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from src.config import get_settings

logger = logging.getLogger("data_governance")


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Create and cache a SQLAlchemy engine singleton.

    Why lru_cache: ensures a single engine instance across the application,
    which is important because each engine manages its own connection pool.
    Multiple engines would waste resources and bypass pool limits.
    """
    settings = get_settings()

    engine = create_engine(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=settings.app_debug,
    )

    logger.info(
        "database_engine_created",
        extra={
            "event": "database_engine_created",
            "pool_size": settings.database_pool_size,
            "max_overflow": settings.database_max_overflow,
        },
    )

    return engine


@contextmanager
def get_connection() -> Generator[Connection, None, None]:
    """Provide a transactional database connection via context manager.

    Why context manager: ensures connections are returned to the pool
    even if an exception occurs, preventing connection leaks under
    error conditions.
    """
    engine = get_engine()
    connection = engine.connect()
    try:
        yield connection
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def check_database_health() -> bool:
    """Verify the database is reachable and responsive.

    Why a dedicated health check: used by the /health endpoint and
    container health probes to detect database connectivity issues
    before they affect user-facing operations.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.error(
            "database_health_check_failed",
            extra={
                "event": "database_health_check_failed",
                "error": str(exc),
            },
        )
        return False
