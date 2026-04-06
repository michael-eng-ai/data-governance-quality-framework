"""Freshness SLA checker.

Why a dedicated freshness module: freshness is the most time-sensitive quality
dimension. Stale data can cause incorrect business decisions within minutes,
so the checker is optimized for fast execution and supports being called
independently from the full quality pipeline (e.g., by Airflow sensors).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.models.contract import DataContract
from src.models.quality import CheckStatus, FreshnessResult

logger = logging.getLogger("data_governance")


class FreshnessChecker:
    """Checks table freshness against SLA thresholds.

    Why SQL-based freshness: querying MAX(timestamp) directly is the most
    reliable way to determine actual data freshness, avoiding metadata
    staleness issues with catalog-based approaches.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def check(self, contract: DataContract) -> FreshnessResult:
        """Check if a table's data is fresh according to its SLA.

        Args:
            contract: Data contract with freshness configuration.

        Returns:
            FreshnessResult with current freshness status and metrics.
        """
        if not contract.freshness:
            return FreshnessResult(
                table_name=contract.table_name,
                schema_name=contract.schema_name,
                timestamp_column="",
                max_delay_minutes=0,
                status=CheckStatus.SKIPPED,
                message="No freshness SLA defined in contract",
            )

        ts_col = contract.freshness.timestamp_column
        max_delay = contract.freshness.max_delay_minutes

        if not self._validate_column_name(ts_col):
            return FreshnessResult(
                table_name=contract.table_name,
                schema_name=contract.schema_name,
                timestamp_column=ts_col,
                max_delay_minutes=max_delay,
                status=CheckStatus.ERROR,
                message=f"Invalid timestamp column name: '{ts_col}'",
            )

        return self._execute_freshness_check(contract, ts_col, max_delay)

    def check_multiple(self, contracts: list[DataContract]) -> list[FreshnessResult]:
        """Check freshness for multiple tables in a single pass.

        Args:
            contracts: List of data contracts to check.

        Returns:
            List of FreshnessResult for each contract.
        """
        return [self.check(contract) for contract in contracts]

    def get_stale_tables(self, contracts: list[DataContract]) -> list[FreshnessResult]:
        """Return only the tables that are outside their freshness SLA.

        Args:
            contracts: List of data contracts to check.

        Returns:
            List of FreshnessResult for tables that failed the freshness check.
        """
        all_results = self.check_multiple(contracts)
        return [r for r in all_results if r.status == CheckStatus.FAILED]

    def _execute_freshness_check(
        self,
        contract: DataContract,
        ts_col: str,
        max_delay: int,
    ) -> FreshnessResult:
        """Execute the freshness query and evaluate against the SLA."""
        qualified_table = f"{contract.schema_name}.{contract.table_name}"

        try:
            if not self._table_exists(contract.schema_name, contract.table_name):
                return FreshnessResult(
                    table_name=contract.table_name,
                    schema_name=contract.schema_name,
                    timestamp_column=ts_col,
                    max_delay_minutes=max_delay,
                    status=CheckStatus.ERROR,
                    message=f"Table {qualified_table} does not exist",
                )

            query = text(f"SELECT MAX({ts_col}) FROM {qualified_table}")  # noqa: S608
            row = self._connection.execute(query).fetchone()
        except Exception as exc:
            logger.error(
                "freshness_check_failed",
                extra={
                    "event": "freshness_check_failed",
                    "table": contract.table_name,
                    "error": str(exc),
                },
            )
            return FreshnessResult(
                table_name=contract.table_name,
                schema_name=contract.schema_name,
                timestamp_column=ts_col,
                max_delay_minutes=max_delay,
                status=CheckStatus.ERROR,
                message=f"Freshness query failed: {exc}",
            )

        if not row or row[0] is None:
            return FreshnessResult(
                table_name=contract.table_name,
                schema_name=contract.schema_name,
                timestamp_column=ts_col,
                max_delay_minutes=max_delay,
                status=CheckStatus.FAILED,
                message=f"Table {qualified_table} has no data or null timestamps",
            )

        last_updated = self._normalize_timestamp(row[0])
        now = datetime.now(timezone.utc)
        delay_minutes = (now - last_updated).total_seconds() / 60.0

        is_fresh = delay_minutes <= max_delay
        status = CheckStatus.PASSED if is_fresh else CheckStatus.FAILED

        logger.info(
            "freshness_checked",
            extra={
                "event": "freshness_checked",
                "table": contract.table_name,
                "status": status.value,
                "actual_delay_minutes": round(delay_minutes, 2),
                "max_delay_minutes": max_delay,
            },
        )

        return FreshnessResult(
            table_name=contract.table_name,
            schema_name=contract.schema_name,
            timestamp_column=ts_col,
            last_updated_at=last_updated,
            max_delay_minutes=max_delay,
            actual_delay_minutes=round(delay_minutes, 2),
            status=status,
            message=(
                f"Data is {round(delay_minutes, 1)} min old "
                f"(SLA: {max_delay} min) - "
                f"{'WITHIN' if is_fresh else 'OUTSIDE'} SLA"
            ),
        )

    def _table_exists(self, schema_name: str, table_name: str) -> bool:
        """Check if the target table exists."""
        query = text(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.tables"
            "  WHERE table_schema = :schema_name"
            "  AND table_name = :table_name"
            ")"
        )
        row = self._connection.execute(
            query,
            {"schema_name": schema_name, "table_name": table_name},
        ).fetchone()
        return bool(row and row[0])

    @staticmethod
    def _validate_column_name(column_name: str) -> bool:
        """Validate column name to prevent SQL injection."""
        return bool(column_name) and column_name.replace("_", "").isalnum()

    @staticmethod
    def _normalize_timestamp(value: datetime | str) -> datetime:
        """Ensure the timestamp is timezone-aware UTC."""
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value
