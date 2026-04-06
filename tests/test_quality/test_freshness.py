"""Tests for the freshness SLA checker.

Why test freshness separately: freshness is the most time-sensitive quality
dimension with the most complex edge cases (timezone handling, null timestamps,
missing tables). Each scenario needs explicit coverage.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from src.models.contract import DataContract, ColumnDefinition, ColumnType, FreshnessConfig
from src.models.quality import CheckStatus
from src.quality.freshness import FreshnessChecker


@pytest.fixture
def freshness_contract() -> DataContract:
    """Contract with freshness SLA for testing."""
    return DataContract(
        table_name="fresh_table",
        owner="test",
        columns=[
            ColumnDefinition(name="id", column_type=ColumnType.INTEGER),
            ColumnDefinition(name="updated_at", column_type=ColumnType.TIMESTAMP),
        ],
        freshness=FreshnessConfig(
            timestamp_column="updated_at",
            max_delay_minutes=60,
        ),
    )


class TestFreshnessChecker:
    """Tests for freshness check execution."""

    def test_check_fresh_data_passes(
        self, freshness_contract: DataContract, mock_connection: MagicMock
    ) -> None:
        # Arrange
        checker = FreshnessChecker(mock_connection)
        recent_time = datetime.now(timezone.utc) - timedelta(minutes=15)

        # table_exists
        exists_row = MagicMock()
        exists_row.__getitem__ = lambda self, idx: True

        # MAX(updated_at)
        ts_row = MagicMock()
        ts_row.__getitem__ = lambda self, idx: recent_time

        mock_connection.execute.return_value.fetchone.side_effect = [exists_row, ts_row]

        # Act
        result = checker.check(freshness_contract)

        # Assert
        assert result.status == CheckStatus.PASSED
        assert result.actual_delay_minutes is not None
        assert result.actual_delay_minutes < 60

    def test_check_stale_data_fails(
        self, freshness_contract: DataContract, mock_connection: MagicMock
    ) -> None:
        # Arrange
        checker = FreshnessChecker(mock_connection)
        old_time = datetime.now(timezone.utc) - timedelta(minutes=120)

        exists_row = MagicMock()
        exists_row.__getitem__ = lambda self, idx: True

        ts_row = MagicMock()
        ts_row.__getitem__ = lambda self, idx: old_time

        mock_connection.execute.return_value.fetchone.side_effect = [exists_row, ts_row]

        # Act
        result = checker.check(freshness_contract)

        # Assert
        assert result.status == CheckStatus.FAILED
        assert result.actual_delay_minutes is not None
        assert result.actual_delay_minutes > 60

    def test_check_no_freshness_config_skips(
        self, mock_connection: MagicMock
    ) -> None:
        # Arrange
        contract = DataContract(
            table_name="no_sla",
            owner="test",
            columns=[ColumnDefinition(name="id", column_type=ColumnType.INTEGER)],
        )
        checker = FreshnessChecker(mock_connection)

        # Act
        result = checker.check(contract)

        # Assert
        assert result.status == CheckStatus.SKIPPED

    def test_check_empty_table_fails(
        self, freshness_contract: DataContract, mock_connection: MagicMock
    ) -> None:
        # Arrange
        checker = FreshnessChecker(mock_connection)

        exists_row = MagicMock()
        exists_row.__getitem__ = lambda self, idx: True

        null_row = MagicMock()
        null_row.__getitem__ = lambda self, idx: None

        mock_connection.execute.return_value.fetchone.side_effect = [exists_row, null_row]

        # Act
        result = checker.check(freshness_contract)

        # Assert
        assert result.status == CheckStatus.FAILED
        assert "no data" in result.message.lower() or "null" in result.message.lower()

    def test_check_invalid_column_name_returns_error(
        self, mock_connection: MagicMock
    ) -> None:
        # Arrange
        contract = DataContract(
            table_name="bad_col",
            owner="test",
            columns=[ColumnDefinition(name="id", column_type=ColumnType.INTEGER)],
            freshness=FreshnessConfig(
                timestamp_column="Robert'; DROP TABLE--",
                max_delay_minutes=60,
            ),
        )
        checker = FreshnessChecker(mock_connection)

        # Act
        result = checker.check(contract)

        # Assert
        assert result.status == CheckStatus.ERROR


class TestFreshnessCheckerMultiple:
    """Tests for batch freshness checking."""

    def test_get_stale_tables_filters_correctly(
        self, mock_connection: MagicMock
    ) -> None:
        # Arrange
        checker = FreshnessChecker(mock_connection)

        fresh_contract = DataContract(
            table_name="fresh",
            owner="test",
            columns=[ColumnDefinition(name="id", column_type=ColumnType.INTEGER)],
        )
        stale_contract = DataContract(
            table_name="stale",
            owner="test",
            columns=[ColumnDefinition(name="id", column_type=ColumnType.INTEGER)],
        )

        # Both have no freshness config, so both skip
        # Act
        stale = checker.get_stale_tables([fresh_contract, stale_contract])

        # Assert
        assert len(stale) == 0  # No freshness SLA = skipped, not failed
