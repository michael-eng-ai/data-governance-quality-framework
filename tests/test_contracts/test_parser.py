"""Tests for the YAML contract parser.

Why test the parser separately from the validator: parsing is the first
line of defense against malformed contracts. If parsing fails silently,
downstream validation may produce misleading results.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.contracts.parser import ContractParser, ContractParserError
from src.models.contract import ColumnType, RuleType


class TestContractParserYAML:
    """Tests for parsing YAML content into DataContract models."""

    def test_parse_valid_yaml_returns_contract(self, sample_orders_yaml: str) -> None:
        # Arrange
        parser = ContractParser()

        # Act
        contract = parser.parse_yaml(sample_orders_yaml)

        # Assert
        assert contract.table_name == "orders"
        assert contract.schema_name == "public"
        assert contract.owner == "data-engineering"
        assert len(contract.columns) == 5
        assert len(contract.quality_rules) == 2

    def test_parse_yaml_extracts_column_types_correctly(
        self, sample_orders_yaml: str
    ) -> None:
        # Arrange
        parser = ContractParser()

        # Act
        contract = parser.parse_yaml(sample_orders_yaml)

        # Assert
        col_types = {col.name: col.column_type for col in contract.columns}
        assert col_types["order_id"] == ColumnType.UUID
        assert col_types["total_amount"] == ColumnType.DECIMAL
        assert col_types["status"] == ColumnType.STRING

    def test_parse_yaml_extracts_freshness_config(
        self, sample_orders_yaml: str
    ) -> None:
        # Arrange
        parser = ContractParser()

        # Act
        contract = parser.parse_yaml(sample_orders_yaml)

        # Assert
        assert contract.freshness is not None
        assert contract.freshness.timestamp_column == "updated_at"
        assert contract.freshness.max_delay_minutes == 120
        assert contract.freshness.check_interval_minutes == 30

    def test_parse_yaml_extracts_quality_rules(
        self, sample_orders_yaml: str
    ) -> None:
        # Arrange
        parser = ContractParser()

        # Act
        contract = parser.parse_yaml(sample_orders_yaml)

        # Assert
        rule_types = [r.rule_type for r in contract.quality_rules]
        assert RuleType.NOT_NULL in rule_types
        assert RuleType.UNIQUE in rule_types

    def test_parse_invalid_yaml_raises_error(self) -> None:
        # Arrange
        parser = ContractParser()
        invalid_yaml = "{{invalid: yaml: content:"

        # Act & Assert
        with pytest.raises(ContractParserError, match="Invalid YAML"):
            parser.parse_yaml(invalid_yaml)

    def test_parse_yaml_missing_columns_raises_error(self) -> None:
        # Arrange
        parser = ContractParser()
        yaml_without_columns = """
table_name: test_table
owner: test
"""

        # Act & Assert
        with pytest.raises(ContractParserError, match="at least one column"):
            parser.parse_yaml(yaml_without_columns)


class TestContractParserFile:
    """Tests for parsing contract files from the filesystem."""

    def test_parse_file_reads_yaml_correctly(
        self, contracts_directory: Path
    ) -> None:
        # Arrange
        parser = ContractParser()
        file_path = contracts_directory / "orders.yml"

        # Act
        contract = parser.parse_file(file_path)

        # Assert
        assert contract.table_name == "orders"
        assert contract.owner == "data-engineering"

    def test_parse_nonexistent_file_raises_error(self) -> None:
        # Arrange
        parser = ContractParser()

        # Act & Assert
        with pytest.raises(ContractParserError, match="not found"):
            parser.parse_file(Path("/nonexistent/contract.yml"))

    def test_parse_non_yaml_file_raises_error(self, tmp_path: Path) -> None:
        # Arrange
        parser = ContractParser()
        txt_file = tmp_path / "contract.txt"
        txt_file.write_text("not yaml")

        # Act & Assert
        with pytest.raises(ContractParserError, match="must be .yml"):
            parser.parse_file(txt_file)


class TestContractParserDirectory:
    """Tests for parsing all contracts in a directory."""

    def test_parse_directory_returns_all_contracts(
        self, contracts_directory: Path
    ) -> None:
        # Arrange
        parser = ContractParser()

        # Act
        contracts = parser.parse_directory(contracts_directory)

        # Assert
        assert len(contracts) >= 1
        assert contracts[0].table_name == "orders"

    def test_parse_nonexistent_directory_raises_error(self) -> None:
        # Arrange
        parser = ContractParser()

        # Act & Assert
        with pytest.raises(ContractParserError, match="not found"):
            parser.parse_directory(Path("/nonexistent/"))
