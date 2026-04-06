"""Quality check result models.

Why dedicated result models: standardizes the output format across different
quality engines (Great Expectations, Soda) so downstream consumers like the
governance dashboard and alerting system don't need engine-specific logic.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class CheckStatus(StrEnum):
    """Outcome of a quality check execution."""

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    ERROR = "error"
    SKIPPED = "skipped"


class CheckResult(BaseModel):
    """Result of a single quality check execution.

    Why granular per-check results: enables targeted remediation by
    identifying exactly which rule failed on which column, rather than
    just reporting a table-level pass/fail.
    """

    check_id: UUID = Field(default_factory=uuid4)
    check_name: str
    check_type: str
    column: str | None = None
    status: CheckStatus
    expected_value: str | None = None
    actual_value: str | None = None
    severity: str = "error"
    message: str | None = None
    executed_at: datetime = Field(default_factory=datetime.utcnow)


class FreshnessResult(BaseModel):
    """Result of a freshness SLA check.

    Why a dedicated model: freshness has unique attributes (delay, SLA)
    that don't fit neatly into the generic CheckResult model, and
    freshness breaches require different alerting logic.
    """

    table_name: str
    schema_name: str = "public"
    timestamp_column: str
    last_updated_at: datetime | None = None
    max_delay_minutes: int
    actual_delay_minutes: float | None = None
    status: CheckStatus
    message: str | None = None
    checked_at: datetime = Field(default_factory=datetime.utcnow)

    @property
    def is_within_sla(self) -> bool:
        """Check if the table freshness is within the defined SLA."""
        if self.actual_delay_minutes is None:
            return False
        return self.actual_delay_minutes <= self.max_delay_minutes


class ValidationResult(BaseModel):
    """Aggregated result of all validations for a single table.

    Why aggregate results: provides a single pass/fail decision for
    pipeline gates while still preserving individual check details
    for debugging and auditing.
    """

    run_id: UUID = Field(default_factory=uuid4)
    table_name: str
    schema_name: str = "public"
    contract_version: str
    overall_status: CheckStatus
    schema_checks: list[CheckResult] = Field(default_factory=list)
    quality_checks: list[CheckResult] = Field(default_factory=list)
    freshness_result: FreshnessResult | None = None
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    warning_checks: int = 0
    duration_seconds: float = 0.0
    executed_at: datetime = Field(default_factory=datetime.utcnow)

    def compute_summary(self) -> None:
        """Recalculate summary counts from individual check results.

        Why a method instead of computed property: this is called once after
        all checks complete rather than on every access, avoiding repeated
        list traversals.
        """
        all_checks = self.schema_checks + self.quality_checks
        self.total_checks = len(all_checks)
        self.passed_checks = sum(1 for c in all_checks if c.status == CheckStatus.PASSED)
        self.failed_checks = sum(1 for c in all_checks if c.status == CheckStatus.FAILED)
        self.warning_checks = sum(1 for c in all_checks if c.status == CheckStatus.WARNING)

        if self.freshness_result:
            self.total_checks += 1
            if self.freshness_result.status == CheckStatus.PASSED:
                self.passed_checks += 1
            elif self.freshness_result.status == CheckStatus.FAILED:
                self.failed_checks += 1

        if self.failed_checks > 0:
            self.overall_status = CheckStatus.FAILED
        elif self.warning_checks > 0:
            self.overall_status = CheckStatus.WARNING
        else:
            self.overall_status = CheckStatus.PASSED


class QualityReport(BaseModel):
    """Summary report of quality check results across multiple tables.

    Why a report model: enables serialization to JSON for API responses
    and persistent storage for trend analysis.
    """

    report_id: UUID = Field(default_factory=uuid4)
    results: list[ValidationResult] = Field(default_factory=list)
    total_tables: int = 0
    tables_passed: int = 0
    tables_failed: int = 0
    generated_at: datetime = Field(default_factory=datetime.utcnow)

    def compute_summary(self) -> None:
        """Recalculate report-level summary from individual results."""
        self.total_tables = len(self.results)
        self.tables_passed = sum(
            1 for r in self.results if r.overall_status == CheckStatus.PASSED
        )
        self.tables_failed = sum(
            1 for r in self.results if r.overall_status == CheckStatus.FAILED
        )
