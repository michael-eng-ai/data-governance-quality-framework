"""Airflow DAG for the data governance pipeline.

Why orchestrate via Airflow: provides scheduling, retries, dependency
management, and observability for the governance pipeline. Each task
maps to a distinct governance concern (sync, validate, report) enabling
independent monitoring and retry.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("data_governance")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def sync_contracts_task() -> int:
    """Synchronize YAML contract files to the database registry.

    Why a separate sync step: ensures the registry is up-to-date before
    running quality checks. Contracts may change between DAG runs.
    """
    from src.db.session import get_connection
    from src.quality.engine import QualityEngine

    with get_connection() as conn:
        engine = QualityEngine(connection=conn)
        count = engine.sync_contracts_from_directory()

    logger.info(
        "dag_contracts_synced",
        extra={"event": "dag_contracts_synced", "count": count},
    )
    return count


def run_quality_checks_task() -> dict[str, int]:
    """Execute quality checks for all registered contracts.

    Why run all checks in a single task: ensures consistent timing for
    the governance snapshot. Running checks individually would create
    timing skew in the metrics.
    """
    from src.db.session import get_connection
    from src.governance.alerts import AlertManager
    from src.quality.engine import QualityEngine

    with get_connection() as conn:
        alert_manager = AlertManager.from_settings()
        engine = QualityEngine(connection=conn, alert_manager=alert_manager)
        report = engine.run_checks_all()

    summary = {
        "total_tables": report.total_tables,
        "tables_passed": report.tables_passed,
        "tables_failed": report.tables_failed,
    }

    logger.info(
        "dag_quality_checks_complete",
        extra={"event": "dag_quality_checks_complete", **summary},
    )
    return summary


def capture_governance_metrics_task() -> dict[str, float]:
    """Capture a governance metrics snapshot after quality checks complete.

    Why after quality checks: the snapshot should reflect the most recent
    check results. Capturing before checks would use stale data.
    """
    from src.db.session import get_connection
    from src.governance.dashboard import GovernanceDashboard

    with get_connection() as conn:
        dashboard = GovernanceDashboard(conn)
        metrics = dashboard.capture_snapshot()

    summary = {
        "contract_coverage_pct": metrics.contract_coverage_pct,
        "quality_pass_rate_pct": metrics.quality_pass_rate_pct,
        "sla_compliance_pct": metrics.sla_compliance_pct,
    }

    logger.info(
        "dag_metrics_captured",
        extra={"event": "dag_metrics_captured", **summary},
    )
    return summary


def generate_report_task() -> dict[str, int]:
    """Generate a summary quality report for the current run.

    Why a separate report task: decouples report generation from check
    execution. Reports may involve additional formatting or delivery
    steps that should not block the main pipeline.
    """
    from src.db.session import get_connection
    from src.governance.reporter import QualityReporter

    with get_connection() as conn:
        reporter = QualityReporter(conn)
        report = reporter.generate_summary_report()

    summary = {
        "total_tables": report.total_tables,
        "tables_passed": report.tables_passed,
        "tables_failed": report.tables_failed,
    }

    logger.info(
        "dag_report_generated",
        extra={"event": "dag_report_generated", **summary},
    )
    return summary


with DAG(
    dag_id="data_governance_pipeline",
    default_args=DEFAULT_ARGS,
    description="Automated data governance: contract sync, quality checks, and metrics",
    schedule="0 */2 * * *",  # Every 2 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["governance", "quality", "data-contracts"],
) as dag:
    sync_contracts = PythonOperator(
        task_id="sync_contracts",
        python_callable=sync_contracts_task,
    )

    run_quality_checks = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks_task,
    )

    capture_metrics = PythonOperator(
        task_id="capture_governance_metrics",
        python_callable=capture_governance_metrics_task,
    )

    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report_task,
    )

    # Task dependencies: sync -> checks -> (metrics + report)
    sync_contracts >> run_quality_checks >> [capture_metrics, generate_report]
