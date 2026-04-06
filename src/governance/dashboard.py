"""Governance metrics collector and dashboard data provider.

Why a separate dashboard module: aggregates metrics from multiple sources
(contracts registry, quality results, freshness checks) into a single
coherent view. This avoids having the API layer do complex cross-table
joins and keeps governance logic testable independently.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.models.governance import GovernanceMetrics, GovernanceTrend

logger = logging.getLogger("data_governance")


class GovernanceDashboard:
    """Collects and persists governance metrics snapshots.

    Why snapshot-based approach: computing metrics on-the-fly from raw data
    becomes expensive as the number of tables and check results grows.
    Pre-computed snapshots enable fast dashboard loads and trend queries.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def capture_snapshot(self) -> GovernanceMetrics:
        """Capture current governance metrics and persist them.

        Returns:
            GovernanceMetrics snapshot with computed percentages.
        """
        total_tables = self._count_total_tables()
        tables_with_contracts = self._count_tables_with_contracts()
        total_checks, passed_checks = self._count_check_results()
        tables_within_sla, tables_with_freshness = self._count_freshness_compliance()

        metrics = GovernanceMetrics.compute(
            total_tables=total_tables,
            tables_with_contracts=tables_with_contracts,
            total_checks_run=total_checks,
            total_checks_passed=passed_checks,
            tables_within_sla=tables_within_sla,
            tables_with_freshness_sla=tables_with_freshness,
        )

        self._persist_snapshot(metrics)

        logger.info(
            "governance_snapshot_captured",
            extra={
                "event": "governance_snapshot_captured",
                "contract_coverage_pct": metrics.contract_coverage_pct,
                "quality_pass_rate_pct": metrics.quality_pass_rate_pct,
                "sla_compliance_pct": metrics.sla_compliance_pct,
            },
        )

        return metrics

    def get_current_metrics(self) -> GovernanceMetrics:
        """Get the most recent governance metrics snapshot.

        Returns:
            Most recent GovernanceMetrics or a fresh capture if none exists.
        """
        query = text(
            "SELECT metric_id, contract_coverage_pct, quality_pass_rate_pct,"
            "       sla_compliance_pct, total_tables, tables_with_contracts,"
            "       total_checks_run, total_checks_passed,"
            "       tables_within_sla, tables_with_freshness_sla, captured_at"
            " FROM governance_metrics"
            " ORDER BY captured_at DESC"
            " LIMIT 1"
        )
        row = self._connection.execute(query).mappings().fetchone()

        if not row:
            return self.capture_snapshot()

        return GovernanceMetrics(
            metric_id=row["metric_id"],
            contract_coverage_pct=row["contract_coverage_pct"],
            quality_pass_rate_pct=row["quality_pass_rate_pct"],
            sla_compliance_pct=row["sla_compliance_pct"],
            total_tables=row["total_tables"],
            tables_with_contracts=row["tables_with_contracts"],
            total_checks_run=row["total_checks_run"],
            total_checks_passed=row["total_checks_passed"],
            tables_within_sla=row["tables_within_sla"],
            tables_with_freshness_sla=row["tables_with_freshness_sla"],
            captured_at=row["captured_at"],
        )

    def get_trends(self, days: int = 30) -> GovernanceTrend:
        """Get governance metrics trend over the specified period.

        Args:
            days: Number of days to look back for trend data.

        Returns:
            GovernanceTrend with historical snapshots.
        """
        query = text(
            "SELECT metric_id, contract_coverage_pct, quality_pass_rate_pct,"
            "       sla_compliance_pct, total_tables, tables_with_contracts,"
            "       total_checks_run, total_checks_passed,"
            "       tables_within_sla, tables_with_freshness_sla, captured_at"
            " FROM governance_metrics"
            " WHERE captured_at >= NOW() - INTERVAL ':days days'"
            " ORDER BY captured_at ASC"
        )

        # Use parameterized interval calculation instead of string interpolation
        safe_query = text(
            "SELECT metric_id, contract_coverage_pct, quality_pass_rate_pct,"
            "       sla_compliance_pct, total_tables, tables_with_contracts,"
            "       total_checks_run, total_checks_passed,"
            "       tables_within_sla, tables_with_freshness_sla, captured_at"
            " FROM governance_metrics"
            " WHERE captured_at >= NOW() - make_interval(days => :days)"
            " ORDER BY captured_at ASC"
        )
        rows = self._connection.execute(safe_query, {"days": days}).mappings().fetchall()

        trend = GovernanceTrend()
        for row in rows:
            snapshot = GovernanceMetrics(
                metric_id=row["metric_id"],
                contract_coverage_pct=row["contract_coverage_pct"],
                quality_pass_rate_pct=row["quality_pass_rate_pct"],
                sla_compliance_pct=row["sla_compliance_pct"],
                total_tables=row["total_tables"],
                tables_with_contracts=row["tables_with_contracts"],
                total_checks_run=row["total_checks_run"],
                total_checks_passed=row["total_checks_passed"],
                tables_within_sla=row["tables_within_sla"],
                tables_with_freshness_sla=row["tables_with_freshness_sla"],
                captured_at=row["captured_at"],
            )
            trend.add_snapshot(snapshot)

        return trend

    def _count_total_tables(self) -> int:
        """Count all user tables in the database."""
        query = text(
            "SELECT COUNT(*) FROM information_schema.tables"
            " WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
            " AND table_type = 'BASE TABLE'"
        )
        row = self._connection.execute(query).fetchone()
        return row[0] if row else 0

    def _count_tables_with_contracts(self) -> int:
        """Count tables that have registered data contracts."""
        query = text("SELECT COUNT(*) FROM data_contracts")
        row = self._connection.execute(query).fetchone()
        return row[0] if row else 0

    def _count_check_results(self) -> tuple[int, int]:
        """Count total and passed checks from the most recent run per table."""
        query = text(
            "SELECT COALESCE(SUM(total_checks), 0) AS total,"
            "       COALESCE(SUM(passed_checks), 0) AS passed"
            " FROM ("
            "   SELECT DISTINCT ON (table_name, schema_name)"
            "          total_checks, passed_checks"
            "   FROM quality_results"
            "   ORDER BY table_name, schema_name, executed_at DESC"
            " ) latest"
        )
        row = self._connection.execute(query).fetchone()
        if not row:
            return 0, 0
        return int(row[0]), int(row[1])

    def _count_freshness_compliance(self) -> tuple[int, int]:
        """Count tables within SLA and total tables with freshness SLAs.

        Returns:
            Tuple of (tables_within_sla, tables_with_freshness_sla).
        """
        query = text(
            "SELECT COUNT(*) AS total_with_freshness,"
            "       COALESCE(SUM(CASE WHEN overall_status = 'passed' THEN 1 ELSE 0 END), 0)"
            "           AS within_sla"
            " FROM ("
            "   SELECT DISTINCT ON (table_name, schema_name)"
            "          overall_status"
            "   FROM quality_results"
            "   WHERE result_data::text LIKE '%freshness%'"
            "   ORDER BY table_name, schema_name, executed_at DESC"
            " ) latest"
        )
        row = self._connection.execute(query).fetchone()
        if not row:
            return 0, 0
        return int(row[1]), int(row[0])

    def _persist_snapshot(self, metrics: GovernanceMetrics) -> None:
        """Store a governance metrics snapshot in the database."""
        query = text(
            "INSERT INTO governance_metrics"
            " (metric_id, contract_coverage_pct, quality_pass_rate_pct,"
            "  sla_compliance_pct, total_tables, tables_with_contracts,"
            "  total_checks_run, total_checks_passed,"
            "  tables_within_sla, tables_with_freshness_sla, captured_at)"
            " VALUES (:metric_id, :contract_coverage_pct, :quality_pass_rate_pct,"
            "  :sla_compliance_pct, :total_tables, :tables_with_contracts,"
            "  :total_checks_run, :total_checks_passed,"
            "  :tables_within_sla, :tables_with_freshness_sla, :captured_at)"
        )
        self._connection.execute(
            query,
            {
                "metric_id": str(metrics.metric_id),
                "contract_coverage_pct": metrics.contract_coverage_pct,
                "quality_pass_rate_pct": metrics.quality_pass_rate_pct,
                "sla_compliance_pct": metrics.sla_compliance_pct,
                "total_tables": metrics.total_tables,
                "tables_with_contracts": metrics.tables_with_contracts,
                "total_checks_run": metrics.total_checks_run,
                "total_checks_passed": metrics.total_checks_passed,
                "tables_within_sla": metrics.tables_within_sla,
                "tables_with_freshness_sla": metrics.tables_with_freshness_sla,
                "captured_at": metrics.captured_at,
            },
        )
        self._connection.commit()
