"""Governance dashboard endpoints.

Why dedicated governance routes: separates governance-level metrics
(coverage, compliance rates) from individual quality results. Governance
data is consumed by executive dashboards that need aggregate views.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter

from src.db.session import get_connection
from src.governance.dashboard import GovernanceDashboard

logger = logging.getLogger("data_governance")

router = APIRouter()


@router.get("/metrics")
def get_governance_metrics() -> dict[str, Any]:
    """Get current governance health metrics.

    Why return the latest snapshot: avoids recomputing metrics on every
    dashboard refresh. Snapshots are captured periodically by the
    governance pipeline DAG.
    """
    with get_connection() as conn:
        dashboard = GovernanceDashboard(conn)
        metrics = dashboard.get_current_metrics()

    return {
        "metrics": {
            "contract_coverage_pct": metrics.contract_coverage_pct,
            "quality_pass_rate_pct": metrics.quality_pass_rate_pct,
            "sla_compliance_pct": metrics.sla_compliance_pct,
            "total_tables": metrics.total_tables,
            "tables_with_contracts": metrics.tables_with_contracts,
            "total_checks_run": metrics.total_checks_run,
            "total_checks_passed": metrics.total_checks_passed,
            "tables_within_sla": metrics.tables_within_sla,
            "tables_with_freshness_sla": metrics.tables_with_freshness_sla,
        },
        "captured_at": metrics.captured_at.isoformat(),
    }


@router.post("/metrics/capture")
def capture_governance_snapshot() -> dict[str, Any]:
    """Capture a new governance metrics snapshot.

    Why an explicit capture endpoint: allows the Airflow DAG and manual
    triggers to capture metrics on demand, independent of the periodic
    snapshot schedule.
    """
    with get_connection() as conn:
        dashboard = GovernanceDashboard(conn)
        metrics = dashboard.capture_snapshot()

    return {
        "status": "captured",
        "metric_id": str(metrics.metric_id),
        "contract_coverage_pct": metrics.contract_coverage_pct,
        "quality_pass_rate_pct": metrics.quality_pass_rate_pct,
        "sla_compliance_pct": metrics.sla_compliance_pct,
        "captured_at": metrics.captured_at.isoformat(),
    }


@router.get("/trends")
def get_governance_trends(days: int = 30) -> dict[str, Any]:
    """Get governance metrics trend over time.

    Args:
        days: Number of days to look back (default: 30).

    Why time-bounded trends: prevents loading unbounded historical data
    that would slow down dashboard rendering and waste bandwidth.
    """
    with get_connection() as conn:
        dashboard = GovernanceDashboard(conn)
        trend = dashboard.get_trends(days=days)

    return {
        "period_start": trend.period_start.isoformat() if trend.period_start else None,
        "period_end": trend.period_end.isoformat() if trend.period_end else None,
        "total_snapshots": len(trend.snapshots),
        "snapshots": [
            {
                "contract_coverage_pct": s.contract_coverage_pct,
                "quality_pass_rate_pct": s.quality_pass_rate_pct,
                "sla_compliance_pct": s.sla_compliance_pct,
                "captured_at": s.captured_at.isoformat(),
            }
            for s in trend.snapshots
        ],
    }
