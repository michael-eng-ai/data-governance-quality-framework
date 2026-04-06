"""Tests for the Great Expectations suite builder and runner.

Why test GE integration: ensures that contract rules correctly translate
to GE expectation configurations. A misconfigured expectation could
silently pass when it should fail, undermining the entire quality framework.
"""

from __future__ import annotations

import pytest

from src.models.contract import DataContract, QualityRule, RuleType
from src.quality.great_expectations import GreatExpectationsRunner


class TestGreatExpectationsSuiteBuilder:
    """Tests for building expectation suites from contracts."""

    def test_build_suite_includes_column_exists(
        self, sample_orders_contract: DataContract
    ) -> None:
        # Arrange
        runner = GreatExpectationsRunner()

        # Act
        expectations = runner.build_expectation_suite(sample_orders_contract)

        # Assert
        exists_expectations = [
            e for e in expectations
            if e["expectation_type"] == "expect_column_to_exist"
        ]
        assert len(exists_expectations) == len(sample_orders_contract.columns)

    def test_build_suite_includes_not_null_for_required_columns(
        self, sample_orders_contract: DataContract
    ) -> None:
        # Arrange
        runner = GreatExpectationsRunner()
        required_cols = sample_orders_contract.get_required_columns()

        # Act
        expectations = runner.build_expectation_suite(sample_orders_contract)

        # Assert
        not_null_expectations = [
            e for e in expectations
            if e["expectation_type"] == "expect_column_values_to_not_be_null"
        ]
        # Should have at least one for each non-nullable column
        assert len(not_null_expectations) >= len(required_cols)

    def test_build_suite_includes_unique_for_primary_keys(
        self, sample_orders_contract: DataContract
    ) -> None:
        # Arrange
        runner = GreatExpectationsRunner()
        pk_cols = sample_orders_contract.get_primary_key_columns()

        # Act
        expectations = runner.build_expectation_suite(sample_orders_contract)

        # Assert
        unique_expectations = [
            e for e in expectations
            if e["expectation_type"] == "expect_column_values_to_be_unique"
        ]
        assert len(unique_expectations) >= len(pk_cols)

    def test_build_suite_maps_accepted_values_rule(
        self, sample_orders_contract: DataContract
    ) -> None:
        # Arrange
        runner = GreatExpectationsRunner()

        # Act
        expectations = runner.build_expectation_suite(sample_orders_contract)

        # Assert
        accepted_expectations = [
            e for e in expectations
            if e["expectation_type"] == "expect_column_values_to_be_in_set"
        ]
        assert len(accepted_expectations) >= 1
        assert "value_set" in accepted_expectations[0]["kwargs"]

    def test_build_suite_maps_min_value_rule(
        self, sample_orders_contract: DataContract
    ) -> None:
        # Arrange
        runner = GreatExpectationsRunner()

        # Act
        expectations = runner.build_expectation_suite(sample_orders_contract)

        # Assert
        between_expectations = [
            e for e in expectations
            if e["expectation_type"] == "expect_column_values_to_be_between"
        ]
        assert len(between_expectations) >= 1


class TestGreatExpectationsRunner:
    """Tests for running expectations and returning results."""

    def test_run_checks_returns_check_results(
        self, sample_orders_contract: DataContract
    ) -> None:
        # Arrange
        runner = GreatExpectationsRunner()

        # Act
        results = runner.run_checks(sample_orders_contract)

        # Assert
        assert len(results) > 0
        assert all(r.check_type == "great_expectations" for r in results)

    def test_run_checks_names_include_expectation_type(
        self, sample_orders_contract: DataContract
    ) -> None:
        # Arrange
        runner = GreatExpectationsRunner()

        # Act
        results = runner.run_checks(sample_orders_contract)

        # Assert
        check_names = [r.check_name for r in results]
        assert any("expect_column_to_exist" in name for name in check_names)

    def test_column_type_mapping_covers_all_types(self) -> None:
        # Arrange & Act
        for type_str in ["string", "integer", "float", "boolean", "date", "timestamp"]:
            result = GreatExpectationsRunner._map_column_type(type_str)

            # Assert
            assert result is not None, f"Missing mapping for {type_str}"
