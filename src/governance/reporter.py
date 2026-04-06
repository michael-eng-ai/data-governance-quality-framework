"""Quality report generator.

Why a separate reporter: transforms raw validation results into structured
reports suitable for different audiences (API consumers, email alerts,
Slack messages). Keeps presentation logic out of the quality engine.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.models.quality import CheckStatus, QualityReport, ValidationResult

logger = logging.getLogger("data_governance")


class QualityReporter:
    """Generates and retrieves quality reports from stored results.

    Why DB-backed reports: enables historical analysis and trend detection
    without keeping all validation results in memory. Also supports
    pagination for large result sets.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def get_recent_results(
        self, limit: int = 50, table_name: str | None = None
    ) -> list[dict[str, Any]]:
        """Retrieve recent quality check results.

        Args:
            limit: Maximum number of results to return.
            table_name: Optional filter by table name.

        Returns:
            List of result summaries ordered by most recent first.
        """
        if table_name:
            query = text(
                "SELECT run_id, table_name, schema_name, contract_version,"
                "       overall_status, total_checks, passed_checks, failed_checks,"
                "       warning_checks, duration_seconds, executed_at"
                " FROM quality_results"
                " WHERE table_name = :table_name"
                " ORDER BY executed_at DESC"
                " LIMIT :limit"
            )
            rows = self._connection.execute(
                query, {"table_name": table_name, "limit": limit}
            ).mappings().fetchall()
        else:
            query = text(
                "SELECT run_id, table_name, schema_name, contract_version,"
                "       overall_status, total_checks, passed_checks, failed_checks,"
                "       warning_checks, duration_seconds, executed_at"
                " FROM quality_results"
                " ORDER BY executed_at DESC"
                " LIMIT :limit"
            )
            rows = self._connection.execute(query, {"limit": limit}).mappings().fetchall()

        return [dict(row) for row in rows]

    def get_result_detail(self, run_id: str) -> dict[str, Any] | None:
        """Retrieve full detail for a specific validation run.

        Args:
            run_id: UUID of the validation run.

        Returns:
            Complete result data or None if not found.
        """
        query = text(
            "SELECT run_id, table_name, schema_name, result_data, executed_at"
            " FROM quality_results"
            " WHERE run_id = :run_id"
        )
        row = self._connection.execute(query, {"run_id": run_id}).mappings().fetchone()

        if not row:
            return None

        result_data: dict[str, Any] = json.loads(row["result_data"])
        return result_data

    def generate_summary_report(self) -> QualityReport:
        """Generate a summary report from the most recent run per table.

        Returns:
            QualityReport with the latest result for each table.
        """
        query = text(
            "SELECT DISTINCT ON (table_name, schema_name)"
            "       run_id, table_name, schema_name, contract_version,"
            "       overall_status, total_checks, passed_checks, failed_checks,"
            "       warning_checks, duration_seconds, result_data, executed_at"
            " FROM quality_results"
            " ORDER BY table_name, schema_name, executed_at DESC"
        )
        rows = self._connection.execute(query).mappings().fetchall()

        report = QualityReport()
        for row in rows:
            result_data = json.loads(row["result_data"])
            result = ValidationResult(**result_data)
            report.results.append(result)

        report.compute_summary()

        logger.info(
            "summary_report_generated",
            extra={
                "event": "summary_report_generated",
                "total_tables": report.total_tables,
                "tables_passed": report.tables_passed,
                "tables_failed": report.tables_failed,
            },
        )

        return report

    def get_failing_tables(self) -> list[dict[str, Any]]:
        """Get tables whose most recent check failed.

        Returns:
            List of failing table summaries with failure details.
        """
        query = text(
            "SELECT DISTINCT ON (table_name, schema_name)"
            "       run_id, table_name, schema_name, contract_version,"
            "       overall_status, failed_checks, executed_at"
            " FROM quality_results"
            " WHERE overall_status = 'failed'"
            " ORDER BY table_name, schema_name, executed_at DESC"
        )
        rows = self._connection.execute(query).mappings().fetchall()
        return [dict(row) for row in rows]

    def get_table_history(
        self, table_name: str, limit: int = 30
    ) -> list[dict[str, Any]]:
        """Get quality check history for a specific table.

        Args:
            table_name: Target table name.
            limit: Maximum number of historical results.

        Returns:
            List of result summaries ordered chronologically.
        """
        query = text(
            "SELECT run_id, overall_status, total_checks, passed_checks,"
            "       failed_checks, duration_seconds, executed_at"
            " FROM quality_results"
            " WHERE table_name = :table_name"
            " ORDER BY executed_at DESC"
            " LIMIT :limit"
        )
        rows = self._connection.execute(
            query, {"table_name": table_name, "limit": limit}
        ).mappings().fetchall()

        return [dict(row) for row in rows]
