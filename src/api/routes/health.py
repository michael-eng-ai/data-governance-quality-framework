"""Health check endpoint.

Why a dedicated health module: container orchestrators (K8s, ECS) and load
balancers require a lightweight endpoint that verifies the application and
its critical dependencies are operational.
"""

from __future__ import annotations

from fastapi import APIRouter

from src.db.session import check_database_health

router = APIRouter()


@router.get("/health")
def health_check() -> dict[str, str | bool]:
    """Return application and dependency health status.

    Why check DB health: a running application that cannot reach its
    database is effectively down. Reporting this allows orchestrators
    to restart the container or route traffic elsewhere.
    """
    db_healthy = check_database_health()
    status = "healthy" if db_healthy else "degraded"

    return {
        "status": status,
        "database": db_healthy,
        "service": "data-governance-quality-framework",
        "version": "1.0.0",
    }
