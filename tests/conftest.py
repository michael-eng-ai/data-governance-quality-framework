"""Shared test fixtures for the data governance framework.

Why centralized fixtures: avoids duplicating setup/teardown logic across
test files and ensures consistent test data across the entire suite.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Generator
from unittest.mock import MagicMock

import pytest

from src.models.contract import (
    ColumnDefinition,
    ColumnType,
    DataContract,
    FreshnessConfig,
    QualityRule,
    RuleType,
)
from src.models.quality import CheckResult, CheckStatus, FreshnessResult, ValidationResult


@pytest.fixture(autouse=True)
def set_test_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set required environment variables for all tests.

    Why autouse: every test needs these settings, and forgetting to set
    them causes confusing failures unrelated to the test logic.
    """
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost:5432/test_db")
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("APP_LOG_LEVEL", "DEBUG")


@pytest.fixture
def sample_orders_contract() -> DataContract:
    """Create a sample orders data contract for testing."""
    return DataContract(
        table_name="orders",
        schema_name="public",
        owner="data-engineering",
        description="Test orders table",
        version="1.0.0",
        columns=[
            ColumnDefinition(
                name="order_id",
                column_type=ColumnType.UUID,
                nullable=False,
                primary_key=True,
            ),
            ColumnDefinition(
                name="customer_id",
                column_type=ColumnType.UUID,
                nullable=False,
            ),
            ColumnDefinition(
                name="total_amount",
                column_type=ColumnType.DECIMAL,
                nullable=False,
            ),
            ColumnDefinition(
                name="status",
                column_type=ColumnType.STRING,
                nullable=False,
            ),
            ColumnDefinition(
                name="updated_at",
                column_type=ColumnType.TIMESTAMP,
                nullable=False,
            ),
        ],
        quality_rules=[
            QualityRule(
                rule_type=RuleType.NOT_NULL,
                column="order_id",
                severity="error",
            ),
            QualityRule(
                rule_type=RuleType.UNIQUE,
                column="order_id",
                severity="error",
            ),
            QualityRule(
                rule_type=RuleType.ACCEPTED_VALUES,
                column="status",
                parameters={"values": ["pending", "completed", "cancelled"]},
                severity="error",
            ),
            QualityRule(
                rule_type=RuleType.MIN_VALUE,
                column="total_amount",
                parameters={"value": 0},
                severity="error",
            ),
        ],
        freshness=FreshnessConfig(
            timestamp_column="updated_at",
            max_delay_minutes=120,
            check_interval_minutes=30,
        ),
        tags=["transactional", "core"],
    )


@pytest.fixture
def sample_customers_contract() -> DataContract:
    """Create a sample customers data contract for testing."""
    return DataContract(
        table_name="customers",
        schema_name="public",
        owner="data-engineering",
        description="Test customers table",
        version="1.0.0",
        columns=[
            ColumnDefinition(
                name="customer_id",
                column_type=ColumnType.UUID,
                nullable=False,
                primary_key=True,
            ),
            ColumnDefinition(
                name="email",
                column_type=ColumnType.STRING,
                nullable=False,
            ),
            ColumnDefinition(
                name="first_name",
                column_type=ColumnType.STRING,
                nullable=False,
            ),
            ColumnDefinition(
                name="country",
                column_type=ColumnType.STRING,
                nullable=True,
            ),
        ],
        quality_rules=[
            QualityRule(
                rule_type=RuleType.UNIQUE,
                column="email",
                severity="error",
            ),
        ],
        tags=["master-data"],
    )


@pytest.fixture
def sample_orders_yaml() -> str:
    """Return sample YAML content for an orders contract."""
    return """
table_name: orders
schema_name: public
owner: data-engineering
description: "Test orders"
version: "1.0.0"
columns:
  - name: order_id
    type: uuid
    nullable: false
    primary_key: true
  - name: customer_id
    type: uuid
    nullable: false
  - name: total_amount
    type: decimal
    nullable: false
  - name: status
    type: string
    nullable: false
  - name: updated_at
    type: timestamp
    nullable: false
quality_rules:
  - rule_type: not_null
    column: order_id
    severity: error
  - rule_type: unique
    column: order_id
    severity: error
freshness:
  timestamp_column: updated_at
  max_delay_minutes: 120
  check_interval_minutes: 30
tags:
  - transactional
"""


@pytest.fixture
def sample_validation_result() -> ValidationResult:
    """Create a sample validation result for testing."""
    result = ValidationResult(
        table_name="orders",
        schema_name="public",
        contract_version="1.0.0",
        overall_status=CheckStatus.PASSED,
        schema_checks=[
            CheckResult(
                check_name="column_exists_order_id",
                check_type="schema",
                column="order_id",
                status=CheckStatus.PASSED,
            ),
            CheckResult(
                check_name="column_type_order_id",
                check_type="schema",
                column="order_id",
                status=CheckStatus.PASSED,
            ),
        ],
        quality_checks=[
            CheckResult(
                check_name="ge_expect_column_values_to_not_be_null",
                check_type="great_expectations",
                column="order_id",
                status=CheckStatus.PASSED,
            ),
        ],
        freshness_result=FreshnessResult(
            table_name="orders",
            timestamp_column="updated_at",
            max_delay_minutes=120,
            actual_delay_minutes=45.0,
            status=CheckStatus.PASSED,
            message="Within SLA",
        ),
        duration_seconds=1.5,
    )
    result.compute_summary()
    return result


@pytest.fixture
def mock_connection() -> MagicMock:
    """Create a mock database connection for testing without DB dependency."""
    connection = MagicMock()
    connection.execute.return_value = MagicMock()
    connection.commit.return_value = None
    return connection


@pytest.fixture
def contracts_directory(tmp_path: Path, sample_orders_yaml: str) -> Path:
    """Create a temporary directory with sample contract files."""
    contract_file = tmp_path / "orders.yml"
    contract_file.write_text(sample_orders_yaml)
    return tmp_path
