"""Contract validation engine.

Why a separate validator from the parser: the parser ensures contract
syntax is correct, while the validator checks that actual data conforms
to the contract. These are distinct concerns with different dependencies
(parser needs YAML, validator needs DB access).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.models.contract import DataContract, ColumnType
from src.models.quality import CheckResult, CheckStatus, FreshnessResult, ValidationResult

logger = logging.getLogger("data_governance")

COLUMN_TYPE_MAP: dict[ColumnType, set[str]] = {
    ColumnType.STRING: {"character varying", "varchar", "text", "char", "character"},
    ColumnType.INTEGER: {"integer", "int", "int4", "bigint", "int8", "smallint", "int2", "serial"},
    ColumnType.FLOAT: {"double precision", "float8", "real", "float4"},
    ColumnType.BOOLEAN: {"boolean", "bool"},
    ColumnType.DATE: {"date"},
    ColumnType.TIMESTAMP: {
        "timestamp without time zone",
        "timestamp with time zone",
        "timestamptz",
        "timestamp",
    },
    ColumnType.DECIMAL: {"numeric", "decimal"},
    ColumnType.TEXT: {"text", "character varying", "varchar"},
    ColumnType.UUID: {"uuid"},
}


class ContractValidationError(Exception):
    """Raised when contract validation encounters an unrecoverable error."""


class ContractValidator:
    """Validates actual database schema and data against a DataContract.

    Why SQL-based validation: works directly against the source of truth
    (the database) rather than relying on cached metadata that could be stale.
    """

    def validate(self, contract: DataContract, connection: Connection) -> ValidationResult:
        """Run all contract validations against the database.

        Args:
            contract: The data contract to validate against.
            connection: Active SQLAlchemy connection to the target database.

        Returns:
            ValidationResult with all check outcomes.
        """
        start_time = time.monotonic()

        result = ValidationResult(
            table_name=contract.table_name,
            schema_name=contract.schema_name,
            contract_version=contract.version,
            overall_status=CheckStatus.PASSED,
        )

        if not self._table_exists(contract, connection):
            result.schema_checks.append(
                CheckResult(
                    check_name="table_exists",
                    check_type="schema",
                    status=CheckStatus.FAILED,
                    message=f"Table {contract.schema_name}.{contract.table_name} does not exist",
                )
            )
            result.overall_status = CheckStatus.FAILED
            result.duration_seconds = time.monotonic() - start_time
            result.compute_summary()
            return result

        schema_checks = self._validate_schema(contract, connection)
        result.schema_checks.extend(schema_checks)

        if contract.freshness:
            freshness_result = self._validate_freshness(contract, connection)
            result.freshness_result = freshness_result

        result.duration_seconds = time.monotonic() - start_time
        result.compute_summary()

        logger.info(
            "contract_validation_complete",
            extra={
                "event": "contract_validation_complete",
                "table": contract.table_name,
                "status": result.overall_status.value,
                "duration_seconds": result.duration_seconds,
                "total_checks": result.total_checks,
                "failed_checks": result.failed_checks,
            },
        )

        return result

    def _table_exists(self, contract: DataContract, connection: Connection) -> bool:
        """Check if the target table exists in the database."""
        query = text(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.tables"
            "  WHERE table_schema = :schema_name"
            "  AND table_name = :table_name"
            ")"
        )
        row = connection.execute(
            query,
            {"schema_name": contract.schema_name, "table_name": contract.table_name},
        ).fetchone()
        return bool(row and row[0])

    def _validate_schema(
        self, contract: DataContract, connection: Connection
    ) -> list[CheckResult]:
        """Validate that database columns match the contract definition.

        Why query information_schema: it is the SQL standard for metadata
        introspection, portable across PostgreSQL versions.
        """
        checks: list[CheckResult] = []

        actual_columns = self._get_actual_columns(contract, connection)

        for expected_col in contract.columns:
            col_name = expected_col.name

            if col_name not in actual_columns:
                checks.append(
                    CheckResult(
                        check_name=f"column_exists_{col_name}",
                        check_type="schema",
                        column=col_name,
                        status=CheckStatus.FAILED,
                        message=f"Column '{col_name}' defined in contract but not found in table",
                    )
                )
                continue

            checks.append(
                CheckResult(
                    check_name=f"column_exists_{col_name}",
                    check_type="schema",
                    column=col_name,
                    status=CheckStatus.PASSED,
                    message=f"Column '{col_name}' exists",
                )
            )

            actual_type = actual_columns[col_name]["data_type"]
            expected_types = COLUMN_TYPE_MAP.get(expected_col.column_type, set())

            if actual_type.lower() not in expected_types:
                checks.append(
                    CheckResult(
                        check_name=f"column_type_{col_name}",
                        check_type="schema",
                        column=col_name,
                        status=CheckStatus.FAILED,
                        expected_value=expected_col.column_type.value,
                        actual_value=actual_type,
                        message=(
                            f"Column '{col_name}' type mismatch: "
                            f"expected {expected_col.column_type.value}, got {actual_type}"
                        ),
                    )
                )
            else:
                checks.append(
                    CheckResult(
                        check_name=f"column_type_{col_name}",
                        check_type="schema",
                        column=col_name,
                        status=CheckStatus.PASSED,
                        expected_value=expected_col.column_type.value,
                        actual_value=actual_type,
                        message=f"Column '{col_name}' type matches contract",
                    )
                )

        return checks

    def _get_actual_columns(
        self, contract: DataContract, connection: Connection
    ) -> dict[str, dict[str, str]]:
        """Fetch actual column metadata from information_schema."""
        query = text(
            "SELECT column_name, data_type, is_nullable"
            " FROM information_schema.columns"
            " WHERE table_schema = :schema_name"
            " AND table_name = :table_name"
            " ORDER BY ordinal_position"
        )
        rows = connection.execute(
            query,
            {"schema_name": contract.schema_name, "table_name": contract.table_name},
        ).fetchall()

        return {
            row[0]: {"data_type": row[1], "is_nullable": row[2]}
            for row in rows
        }

    def _validate_freshness(
        self, contract: DataContract, connection: Connection
    ) -> FreshnessResult:
        """Check if the table data is fresh according to the SLA.

        Why MAX query: the most recent timestamp indicates when the table
        was last updated, which is the core freshness metric.
        """
        if not contract.freshness:
            return FreshnessResult(
                table_name=contract.table_name,
                schema_name=contract.schema_name,
                timestamp_column="",
                max_delay_minutes=0,
                status=CheckStatus.SKIPPED,
                message="No freshness SLA defined",
            )

        ts_col = contract.freshness.timestamp_column

        # Validate column name to prevent SQL injection
        if not ts_col.replace("_", "").isalnum():
            return FreshnessResult(
                table_name=contract.table_name,
                schema_name=contract.schema_name,
                timestamp_column=ts_col,
                max_delay_minutes=contract.freshness.max_delay_minutes,
                status=CheckStatus.ERROR,
                message=f"Invalid timestamp column name: {ts_col}",
            )

        qualified_table = f"{contract.schema_name}.{contract.table_name}"
        query = text(f"SELECT MAX({ts_col}) FROM {qualified_table}")  # noqa: S608

        try:
            row = connection.execute(query).fetchone()
        except Exception as exc:
            logger.error(
                "freshness_query_failed",
                extra={
                    "event": "freshness_query_failed",
                    "table": contract.table_name,
                    "error": str(exc),
                },
            )
            return FreshnessResult(
                table_name=contract.table_name,
                schema_name=contract.schema_name,
                timestamp_column=ts_col,
                max_delay_minutes=contract.freshness.max_delay_minutes,
                status=CheckStatus.ERROR,
                message=f"Freshness query failed: {exc}",
            )

        if not row or row[0] is None:
            return FreshnessResult(
                table_name=contract.table_name,
                schema_name=contract.schema_name,
                timestamp_column=ts_col,
                max_delay_minutes=contract.freshness.max_delay_minutes,
                status=CheckStatus.FAILED,
                message="No data found in table (empty or null timestamps)",
            )

        last_updated = row[0]
        if not isinstance(last_updated, datetime):
            last_updated = datetime.fromisoformat(str(last_updated))

        if last_updated.tzinfo is None:
            last_updated = last_updated.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        delay_minutes = (now - last_updated).total_seconds() / 60.0

        is_fresh = delay_minutes <= contract.freshness.max_delay_minutes
        status = CheckStatus.PASSED if is_fresh else CheckStatus.FAILED

        return FreshnessResult(
            table_name=contract.table_name,
            schema_name=contract.schema_name,
            timestamp_column=ts_col,
            last_updated_at=last_updated,
            max_delay_minutes=contract.freshness.max_delay_minutes,
            actual_delay_minutes=round(delay_minutes, 2),
            status=status,
            message=(
                f"Table is {'within' if is_fresh else 'outside'} freshness SLA: "
                f"{round(delay_minutes, 1)} min actual vs "
                f"{contract.freshness.max_delay_minutes} min allowed"
            ),
        )
