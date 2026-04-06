"""Tests for the contract validation engine.

Why mock the database: validators should be tested for their logic
(type mapping, result aggregation) not for database connectivity.
Integration tests with a real database are a separate concern.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta

import pytest

from src.contracts.validator import ContractValidator, COLUMN_TYPE_MAP
from src.models.contract import DataContract, ColumnDefinition, ColumnType, FreshnessConfig
from src.models.quality import CheckStatus


class TestContractValidatorSchema:
    """Tests for schema validation logic."""

    def test_validate_missing_table_returns_failed(
        self, sample_orders_contract: DataContract, mock_connection: MagicMock
    ) -> None:
        # Arrange
        validator = ContractValidator()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: False
        mock_connection.execute.return_value.fetchone.return_value = mock_row

        # Act
        result = validator.validate(sample_orders_contract, mock_connection)

        # Assert
        assert result.overall_status == CheckStatus.FAILED
        assert any("does not exist" in c.message for c in result.schema_checks)

    def test_validate_existing_table_checks_columns(
        self, sample_orders_contract: DataContract, mock_connection: MagicMock
    ) -> None:
        # Arrange
        validator = ContractValidator()

        # First call: table_exists returns True
        exists_row = MagicMock()
        exists_row.__getitem__ = lambda self, idx: True

        # Second call: columns query returns matching columns
        column_rows = [
            ("order_id", "uuid", "NO"),
            ("customer_id", "uuid", "NO"),
            ("total_amount", "numeric", "NO"),
            ("status", "character varying", "NO"),
            ("updated_at", "timestamp with time zone", "NO"),
        ]

        # Third call: freshness query
        freshness_row = MagicMock()
        freshness_row.__getitem__ = lambda self, idx: datetime.now(timezone.utc)

        mock_connection.execute.return_value.fetchone.side_effect = [
            exists_row,  # table_exists
            freshness_row,  # freshness MAX query
        ]
        mock_connection.execute.return_value.fetchall.return_value = column_rows

        # Act
        result = validator.validate(sample_orders_contract, mock_connection)

        # Assert
        assert result.total_checks > 0
        assert result.duration_seconds >= 0

    def test_validate_missing_column_reports_failure(
        self, mock_connection: MagicMock
    ) -> None:
        # Arrange
        validator = ContractValidator()
        contract = DataContract(
            table_name="test_table",
            owner="test",
            columns=[
                ColumnDefinition(name="id", column_type=ColumnType.INTEGER, nullable=False),
                ColumnDefinition(name="missing_col", column_type=ColumnType.STRING),
            ],
        )

        exists_row = MagicMock()
        exists_row.__getitem__ = lambda self, idx: True

        column_rows = [("id", "integer", "NO")]

        mock_connection.execute.return_value.fetchone.return_value = exists_row
        mock_connection.execute.return_value.fetchall.return_value = column_rows

        # Act
        result = validator.validate(contract, mock_connection)

        # Assert
        missing_checks = [
            c for c in result.schema_checks
            if c.status == CheckStatus.FAILED and "missing_col" in (c.column or "")
        ]
        assert len(missing_checks) > 0


class TestContractValidatorFreshness:
    """Tests for freshness validation logic."""

    def test_validate_fresh_data_returns_passed(
        self, mock_connection: MagicMock
    ) -> None:
        # Arrange
        validator = ContractValidator()
        contract = DataContract(
            table_name="fresh_table",
            owner="test",
            columns=[
                ColumnDefinition(name="id", column_type=ColumnType.INTEGER),
            ],
            freshness=FreshnessConfig(
                timestamp_column="updated_at",
                max_delay_minutes=120,
            ),
        )

        exists_row = MagicMock()
        exists_row.__getitem__ = lambda self, idx: True

        recent_time = datetime.now(timezone.utc) - timedelta(minutes=30)
        freshness_row = MagicMock()
        freshness_row.__getitem__ = lambda self, idx: recent_time

        column_rows = [("id", "integer", "NO"), ("updated_at", "timestamp with time zone", "NO")]

        call_count = 0
        def side_effect_fetchone():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return exists_row
            return freshness_row

        mock_connection.execute.return_value.fetchone.side_effect = side_effect_fetchone
        mock_connection.execute.return_value.fetchall.return_value = column_rows

        # Act
        result = validator.validate(contract, mock_connection)

        # Assert
        assert result.freshness_result is not None
        assert result.freshness_result.status == CheckStatus.PASSED

    def test_no_freshness_config_skips_check(
        self, mock_connection: MagicMock
    ) -> None:
        # Arrange
        validator = ContractValidator()
        contract = DataContract(
            table_name="no_freshness",
            owner="test",
            columns=[
                ColumnDefinition(name="id", column_type=ColumnType.INTEGER),
            ],
        )

        exists_row = MagicMock()
        exists_row.__getitem__ = lambda self, idx: True
        mock_connection.execute.return_value.fetchone.return_value = exists_row
        mock_connection.execute.return_value.fetchall.return_value = [("id", "integer", "NO")]

        # Act
        result = validator.validate(contract, mock_connection)

        # Assert
        assert result.freshness_result is None


class TestColumnTypeMapping:
    """Tests for the column type mapping dictionary."""

    def test_string_type_maps_to_varchar(self) -> None:
        # Assert
        assert "character varying" in COLUMN_TYPE_MAP[ColumnType.STRING]

    def test_integer_type_maps_to_multiple_int_types(self) -> None:
        # Assert
        int_types = COLUMN_TYPE_MAP[ColumnType.INTEGER]
        assert "integer" in int_types
        assert "bigint" in int_types

    def test_all_column_types_have_mappings(self) -> None:
        # Assert
        for col_type in ColumnType:
            assert col_type in COLUMN_TYPE_MAP, f"Missing mapping for {col_type}"
