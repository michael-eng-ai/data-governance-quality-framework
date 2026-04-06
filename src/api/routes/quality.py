"""Quality check endpoints.

Why API-triggered quality checks: enables on-demand validation from
dashboards, CI/CD pipelines, and incident response workflows without
requiring Airflow DAG execution.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException

from src.contracts.registry import ContractNotFoundError
from src.db.session import get_connection
from src.governance.alerts import AlertManager
from src.governance.reporter import QualityReporter
from src.quality.engine import QualityEngine

logger = logging.getLogger("data_governance")

router = APIRouter()


@router.post("/run/{table_name}")
def run_quality_checks(table_name: str, schema_name: str = "public") -> dict[str, Any]:
    """Trigger quality checks for a specific table.

    Why synchronous execution: quality checks typically complete within
    seconds for single tables. Async execution would add complexity
    without meaningful benefit for the common case.
    """
    with get_connection() as conn:
        alert_manager = AlertManager.from_settings()
        engine = QualityEngine(
            connection=conn,
            alert_manager=alert_manager,
        )

        try:
            result = engine.run_checks_for_table(table_name, schema_name)
        except ContractNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504,
                detail=f"Quality check timed out for {schema_name}.{table_name}",
            ) from exc

    return {
        "run_id": str(result.run_id),
        "table_name": result.table_name,
        "schema_name": result.schema_name,
        "overall_status": result.overall_status.value,
        "total_checks": result.total_checks,
        "passed_checks": result.passed_checks,
        "failed_checks": result.failed_checks,
        "warning_checks": result.warning_checks,
        "duration_seconds": round(result.duration_seconds, 3),
    }


@router.post("/run-all")
def run_all_quality_checks() -> dict[str, Any]:
    """Trigger quality checks for all registered tables.

    Why a separate endpoint: avoids clients needing to iterate over
    the contract list and make individual requests. Also ensures all
    tables are checked with a consistent connection and timestamp.
    """
    with get_connection() as conn:
        alert_manager = AlertManager.from_settings()
        engine = QualityEngine(
            connection=conn,
            alert_manager=alert_manager,
        )
        report = engine.run_checks_all()

    return {
        "report_id": str(report.report_id),
        "total_tables": report.total_tables,
        "tables_passed": report.tables_passed,
        "tables_failed": report.tables_failed,
        "results": [
            {
                "table_name": r.table_name,
                "status": r.overall_status.value,
                "total_checks": r.total_checks,
                "failed_checks": r.failed_checks,
            }
            for r in report.results
        ],
    }


@router.get("/results")
def list_quality_results(
    limit: int = 50,
    table_name: str | None = None,
) -> dict[str, Any]:
    """List recent quality check results.

    Why pagination via limit: prevents unbounded result sets that could
    overwhelm the client. Default of 50 covers typical dashboard needs.
    """
    with get_connection() as conn:
        reporter = QualityReporter(conn)
        results = reporter.get_recent_results(limit=limit, table_name=table_name)

    return {
        "total": len(results),
        "results": results,
    }


@router.get("/results/{run_id}")
def get_quality_result_detail(run_id: str) -> dict[str, Any]:
    """Get full detail for a specific quality check run.

    Why a detail endpoint: the list endpoint returns summaries for
    efficiency. Full detail includes individual check results and
    is only loaded when a user drills into a specific run.
    """
    with get_connection() as conn:
        reporter = QualityReporter(conn)
        detail = reporter.get_result_detail(run_id)

    if not detail:
        raise HTTPException(
            status_code=404,
            detail=f"Quality result not found for run_id: {run_id}",
        )

    return detail


@router.get("/failing")
def list_failing_tables() -> dict[str, Any]:
    """List tables whose most recent quality check failed.

    Why a dedicated failing endpoint: the most common dashboard action
    is viewing failing tables for remediation. This avoids clients
    filtering the full results list.
    """
    with get_connection() as conn:
        reporter = QualityReporter(conn)
        failing = reporter.get_failing_tables()

    return {
        "total_failing": len(failing),
        "tables": failing,
    }
