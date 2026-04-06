"""Tests for the quality check orchestrator.

Why test the orchestrator: it coordinates multiple engines and handles
error aggregation. Edge cases like partial failures and empty contracts
need coverage to ensure the pipeline does not silently drop errors.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.models.contract import DataContract
from src.models.quality import CheckStatus
from src.quality.engine import QualityEngine


class TestQualityEngineRunChecks:
    """Tests for running quality checks on a single table."""

    def test_run_checks_returns_validation_result(
        self,
        sample_orders_contract: DataContract,
        mock_connection: MagicMock,
    ) -> None:
        # Arrange
        with patch.object(QualityEngine, "_resolve_contract", return_value=sample_orders_contract):
            with patch.object(QualityEngine, "_store_result"):
                engine = QualityEngine(connection=mock_connection)

                # Mock validator to return a basic result
                mock_validator_result = MagicMock()
                mock_validator_result.schema_checks = []
                mock_validator_result.freshness_result = None
                engine._validator.validate = MagicMock(return_value=mock_validator_result)

                # Act
                result = engine.run_checks_for_table("orders", "public")

        # Assert
        assert result.table_name == "orders"
        assert result.total_checks >= 0
        assert result.duration_seconds >= 0

    def test_run_checks_aggregates_all_engine_results(
        self,
        sample_orders_contract: DataContract,
        mock_connection: MagicMock,
    ) -> None:
        # Arrange
        with patch.object(QualityEngine, "_resolve_contract", return_value=sample_orders_contract):
            with patch.object(QualityEngine, "_store_result"):
                engine = QualityEngine(connection=mock_connection)

                mock_validator_result = MagicMock()
                mock_validator_result.schema_checks = []
                mock_validator_result.freshness_result = None
                engine._validator.validate = MagicMock(return_value=mock_validator_result)

                # Act
                result = engine.run_checks_for_table("orders")

        # Assert
        assert result.schema_checks is not None
        assert result.quality_checks is not None

    def test_run_checks_handles_ge_failure_gracefully(
        self,
        sample_orders_contract: DataContract,
        mock_connection: MagicMock,
    ) -> None:
        # Arrange
        with patch.object(QualityEngine, "_resolve_contract", return_value=sample_orders_contract):
            with patch.object(QualityEngine, "_store_result"):
                engine = QualityEngine(connection=mock_connection)

                mock_validator_result = MagicMock()
                mock_validator_result.schema_checks = []
                mock_validator_result.freshness_result = None
                engine._validator.validate = MagicMock(return_value=mock_validator_result)
                engine._ge_runner.run_checks = MagicMock(
                    side_effect=RuntimeError("GE connection failed")
                )

                # Act
                result = engine.run_checks_for_table("orders")

        # Assert
        error_checks = [c for c in result.quality_checks if c.status == CheckStatus.ERROR]
        assert len(error_checks) >= 1


class TestQualityEngineSyncContracts:
    """Tests for synchronizing contracts from directory."""

    def test_sync_contracts_parses_and_registers(
        self,
        mock_connection: MagicMock,
        contracts_directory,
    ) -> None:
        # Arrange
        mock_connection.execute.return_value.mappings.return_value.fetchone.return_value = None
        engine = QualityEngine(
            connection=mock_connection,
            contracts_directory=str(contracts_directory),
        )

        # Act
        count = engine.sync_contracts_from_directory()

        # Assert
        assert count >= 1

    def test_run_all_returns_quality_report(
        self,
        sample_orders_contract: DataContract,
        mock_connection: MagicMock,
    ) -> None:
        # Arrange
        with patch.object(QualityEngine, "run_checks_for_table") as mock_run:
            mock_run.return_value = MagicMock(
                table_name="orders",
                overall_status=CheckStatus.PASSED,
                total_checks=5,
                failed_checks=0,
            )
            mock_connection.execute.return_value.fetchall.return_value = []

            engine = QualityEngine(connection=mock_connection)
            engine._registry.list_all = MagicMock(return_value=[sample_orders_contract])

            # Act
            report = engine.run_checks_all()

        # Assert
        assert report.total_tables == 1
