"""Quality check orchestrator.

Why a central orchestrator: coordinates multiple quality engines (GE, Soda,
custom) in a deterministic order, aggregates results into a unified format,
and handles persistence and alerting as cross-cutting concerns. Individual
engines remain focused on their specific validation logic.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from sqlalchemy.engine import Connection

from src.contracts.parser import ContractParser
from src.contracts.registry import ContractNotFoundError, ContractRegistry
from src.contracts.validator import ContractValidator
from src.governance.alerts import AlertManager
from src.models.contract import DataContract
from src.models.quality import (
    CheckResult,
    CheckStatus,
    QualityReport,
    ValidationResult,
)
from src.quality.freshness import FreshnessChecker
from src.quality.great_expectations import GreatExpectationsRunner
from src.quality.soda_checks import SodaCheckRunner

logger = logging.getLogger("data_governance")


class QualityEngine:
    """Orchestrates quality checks across all validation engines.

    Why constructor injection: enables testing with mock engines and
    supports swapping implementations without modifying orchestration logic.
    """

    def __init__(
        self,
        connection: Connection,
        contracts_directory: str = "./contracts",
        alert_manager: AlertManager | None = None,
    ) -> None:
        self._connection = connection
        self._contracts_directory = Path(contracts_directory)
        self._parser = ContractParser()
        self._registry = ContractRegistry(connection)
        self._validator = ContractValidator()
        self._freshness_checker = FreshnessChecker(connection)
        self._ge_runner = GreatExpectationsRunner()
        self._soda_runner = SodaCheckRunner()
        self._alert_manager = alert_manager

    def run_checks_for_table(self, table_name: str, schema_name: str = "public") -> ValidationResult:
        """Run all quality checks for a single table.

        Args:
            table_name: Target table to validate.
            schema_name: Schema containing the table.

        Returns:
            ValidationResult with all check outcomes.

        Raises:
            ContractNotFoundError: If no contract exists for the table.
        """
        start_time = time.monotonic()

        contract = self._resolve_contract(table_name, schema_name)

        logger.info(
            "quality_check_started",
            extra={
                "event": "quality_check_started",
                "table": table_name,
                "schema": schema_name,
                "contract_version": contract.version,
            },
        )

        # Step 1: Schema validation
        schema_result = self._validator.validate(contract, self._connection)

        # Step 2: Great Expectations checks
        ge_checks = self._run_ge_checks(contract)

        # Step 3: Soda checks
        soda_checks = self._run_soda_checks(contract)

        # Step 4: Freshness check
        freshness_result = None
        if contract.freshness:
            freshness_result = self._freshness_checker.check(contract)

        # Aggregate results
        result = ValidationResult(
            table_name=table_name,
            schema_name=schema_name,
            contract_version=contract.version,
            overall_status=CheckStatus.PASSED,
            schema_checks=schema_result.schema_checks,
            quality_checks=ge_checks + soda_checks,
            freshness_result=freshness_result or schema_result.freshness_result,
            duration_seconds=time.monotonic() - start_time,
        )
        result.compute_summary()

        # Step 5: Persist results
        self._store_result(result)

        # Step 6: Alert on failures
        if result.overall_status == CheckStatus.FAILED and self._alert_manager:
            self._alert_manager.send_quality_alert(result)

        logger.info(
            "quality_check_complete",
            extra={
                "event": "quality_check_complete",
                "table": table_name,
                "status": result.overall_status.value,
                "total_checks": result.total_checks,
                "passed": result.passed_checks,
                "failed": result.failed_checks,
                "duration_seconds": result.duration_seconds,
            },
        )

        return result

    def run_checks_all(self) -> QualityReport:
        """Run quality checks for all registered contracts.

        Returns:
            QualityReport summarizing results across all tables.
        """
        contracts = self._registry.list_all()
        report = QualityReport()

        for contract in contracts:
            try:
                result = self.run_checks_for_table(
                    contract.table_name, contract.schema_name
                )
                report.results.append(result)
            except Exception as exc:
                logger.error(
                    "quality_check_error",
                    extra={
                        "event": "quality_check_error",
                        "table": contract.table_name,
                        "error": str(exc),
                    },
                )
                error_result = ValidationResult(
                    table_name=contract.table_name,
                    schema_name=contract.schema_name,
                    contract_version=contract.version,
                    overall_status=CheckStatus.ERROR,
                )
                report.results.append(error_result)

        report.compute_summary()
        return report

    def sync_contracts_from_directory(self) -> int:
        """Load contracts from YAML files and register them in the database.

        Returns:
            Number of contracts synchronized.
        """
        contracts = self._parser.parse_directory(self._contracts_directory)
        for contract in contracts:
            self._registry.register(contract)

        logger.info(
            "contracts_synced",
            extra={
                "event": "contracts_synced",
                "count": len(contracts),
                "directory": str(self._contracts_directory),
            },
        )
        return len(contracts)

    def _resolve_contract(self, table_name: str, schema_name: str) -> DataContract:
        """Look up a contract by table name, falling back to YAML files."""
        try:
            return self._registry.get(table_name, schema_name)
        except ContractNotFoundError:
            pass

        yaml_path = self._contracts_directory / f"{table_name}.yml"
        if yaml_path.exists():
            contract = self._parser.parse_file(yaml_path)
            self._registry.register(contract)
            return contract

        raise ContractNotFoundError(
            f"No contract found for {schema_name}.{table_name} "
            f"in registry or directory {self._contracts_directory}"
        )

    def _run_ge_checks(self, contract: DataContract) -> list[CheckResult]:
        """Run Great Expectations checks derived from contract rules."""
        try:
            return self._ge_runner.run_checks(contract)
        except Exception as exc:
            logger.error(
                "ge_check_error",
                extra={
                    "event": "ge_check_error",
                    "table": contract.table_name,
                    "error": str(exc),
                },
            )
            return [
                CheckResult(
                    check_name="great_expectations_suite",
                    check_type="quality",
                    status=CheckStatus.ERROR,
                    message=f"Great Expectations execution failed: {exc}",
                )
            ]

    def _run_soda_checks(self, contract: DataContract) -> list[CheckResult]:
        """Run Soda checks derived from contract rules."""
        try:
            return self._soda_runner.run_checks(contract)
        except Exception as exc:
            logger.error(
                "soda_check_error",
                extra={
                    "event": "soda_check_error",
                    "table": contract.table_name,
                    "error": str(exc),
                },
            )
            return [
                CheckResult(
                    check_name="soda_check_suite",
                    check_type="quality",
                    status=CheckStatus.ERROR,
                    message=f"Soda check execution failed: {exc}",
                )
            ]

    def _store_result(self, result: ValidationResult) -> None:
        """Persist a validation result to the database for audit trail."""
        try:
            from sqlalchemy import text

            query = text(
                "INSERT INTO quality_results"
                " (run_id, table_name, schema_name, contract_version,"
                "  overall_status, total_checks, passed_checks, failed_checks,"
                "  warning_checks, duration_seconds, result_data, executed_at)"
                " VALUES (:run_id, :table_name, :schema_name, :contract_version,"
                "  :overall_status, :total_checks, :passed_checks, :failed_checks,"
                "  :warning_checks, :duration_seconds, :result_data, :executed_at)"
            )
            self._connection.execute(
                query,
                {
                    "run_id": str(result.run_id),
                    "table_name": result.table_name,
                    "schema_name": result.schema_name,
                    "contract_version": result.contract_version,
                    "overall_status": result.overall_status.value,
                    "total_checks": result.total_checks,
                    "passed_checks": result.passed_checks,
                    "failed_checks": result.failed_checks,
                    "warning_checks": result.warning_checks,
                    "duration_seconds": result.duration_seconds,
                    "result_data": result.model_dump_json(),
                    "executed_at": result.executed_at,
                },
            )
            self._connection.commit()
        except Exception as exc:
            logger.error(
                "result_storage_failed",
                extra={
                    "event": "result_storage_failed",
                    "table": result.table_name,
                    "error": str(exc),
                },
            )
