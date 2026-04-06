"""Governance metrics models.

Why dedicated governance models: separates the time-series governance metrics
from the raw quality check results, enabling efficient trend queries without
scanning the entire results history.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class GovernanceMetrics(BaseModel):
    """Point-in-time snapshot of governance health metrics.

    Why snapshot-based metrics: captures the state at a specific moment
    for trend analysis, rather than recomputing from raw data on every
    dashboard load which would be expensive at scale.
    """

    metric_id: UUID = Field(default_factory=uuid4)
    contract_coverage_pct: float = Field(ge=0.0, le=100.0)
    quality_pass_rate_pct: float = Field(ge=0.0, le=100.0)
    sla_compliance_pct: float = Field(ge=0.0, le=100.0)
    total_tables: int = Field(ge=0)
    tables_with_contracts: int = Field(ge=0)
    total_checks_run: int = Field(ge=0)
    total_checks_passed: int = Field(ge=0)
    tables_within_sla: int = Field(ge=0)
    tables_with_freshness_sla: int = Field(ge=0)
    captured_at: datetime = Field(default_factory=datetime.utcnow)

    @classmethod
    def compute(
        cls,
        total_tables: int,
        tables_with_contracts: int,
        total_checks_run: int,
        total_checks_passed: int,
        tables_within_sla: int,
        tables_with_freshness_sla: int,
    ) -> GovernanceMetrics:
        """Factory that computes percentages from raw counts.

        Why a factory method: encapsulates the percentage calculation logic
        and handles division-by-zero cases in a single place.
        """
        contract_coverage = (
            (tables_with_contracts / total_tables * 100.0) if total_tables > 0 else 0.0
        )
        quality_pass_rate = (
            (total_checks_passed / total_checks_run * 100.0) if total_checks_run > 0 else 0.0
        )
        sla_compliance = (
            (tables_within_sla / tables_with_freshness_sla * 100.0)
            if tables_with_freshness_sla > 0
            else 100.0
        )

        return cls(
            contract_coverage_pct=round(contract_coverage, 2),
            quality_pass_rate_pct=round(quality_pass_rate, 2),
            sla_compliance_pct=round(sla_compliance, 2),
            total_tables=total_tables,
            tables_with_contracts=tables_with_contracts,
            total_checks_run=total_checks_run,
            total_checks_passed=total_checks_passed,
            tables_within_sla=tables_within_sla,
            tables_with_freshness_sla=tables_with_freshness_sla,
        )


class GovernanceTrend(BaseModel):
    """Time series of governance metrics for trend visualization.

    Why a list of snapshots: enables charting metrics over time in the
    governance dashboard without complex time-series aggregation queries.
    """

    snapshots: list[GovernanceMetrics] = Field(default_factory=list)
    period_start: datetime | None = None
    period_end: datetime | None = None

    def add_snapshot(self, snapshot: GovernanceMetrics) -> None:
        """Append a snapshot and update the time range bounds."""
        self.snapshots.append(snapshot)
        self.snapshots.sort(key=lambda s: s.captured_at)
        if self.snapshots:
            self.period_start = self.snapshots[0].captured_at
            self.period_end = self.snapshots[-1].captured_at
