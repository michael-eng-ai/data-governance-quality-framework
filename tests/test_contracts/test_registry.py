"""Tests for the contract registry.

Why test registry separately: registry operations (CRUD) have different
failure modes than parsing or validation. Mock the DB to test the SQL
generation and result mapping logic.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from src.contracts.registry import ContractNotFoundError, ContractRegistry
from src.models.contract import DataContract


class TestContractRegistryRegister:
    """Tests for contract registration (insert/update)."""

    def test_register_new_contract_inserts_row(
        self,
        sample_orders_contract: DataContract,
        mock_connection: MagicMock,
    ) -> None:
        # Arrange
        mock_connection.execute.return_value.mappings.return_value.fetchone.return_value = None
        registry = ContractRegistry(mock_connection)

        # Act
        result = registry.register(sample_orders_contract)

        # Assert
        assert result.table_name == "orders"
        assert result.created_at is not None
        assert mock_connection.commit.called

    def test_register_existing_contract_updates_row(
        self,
        sample_orders_contract: DataContract,
        mock_connection: MagicMock,
    ) -> None:
        # Arrange
        existing_row = {
            "table_name": "orders",
            "schema_name": "public",
            "owner": "data-engineering",
            "version": "0.9.0",
            "contract_data": sample_orders_contract.model_dump_json(),
            "created_at": "2024-01-01T00:00:00+00:00",
            "updated_at": "2024-01-01T00:00:00+00:00",
        }
        mock_connection.execute.return_value.mappings.return_value.fetchone.return_value = (
            existing_row
        )
        registry = ContractRegistry(mock_connection)

        # Act
        result = registry.register(sample_orders_contract)

        # Assert
        assert result.updated_at is not None
        assert mock_connection.commit.called

    def test_register_sets_timestamps(
        self,
        sample_orders_contract: DataContract,
        mock_connection: MagicMock,
    ) -> None:
        # Arrange
        mock_connection.execute.return_value.mappings.return_value.fetchone.return_value = None
        registry = ContractRegistry(mock_connection)

        # Act
        result = registry.register(sample_orders_contract)

        # Assert
        assert result.created_at is not None
        assert result.updated_at is not None


class TestContractRegistryGet:
    """Tests for contract retrieval."""

    def test_get_existing_contract_returns_model(
        self,
        sample_orders_contract: DataContract,
        mock_connection: MagicMock,
    ) -> None:
        # Arrange
        row_data = {
            "table_name": "orders",
            "schema_name": "public",
            "owner": "data-engineering",
            "version": "1.0.0",
            "contract_data": sample_orders_contract.model_dump_json(),
            "created_at": "2024-01-01T00:00:00+00:00",
            "updated_at": "2024-01-01T00:00:00+00:00",
        }
        mock_connection.execute.return_value.mappings.return_value.fetchone.return_value = row_data
        registry = ContractRegistry(mock_connection)

        # Act
        contract = registry.get("orders", "public")

        # Assert
        assert contract.table_name == "orders"
        assert len(contract.columns) == 5

    def test_get_nonexistent_contract_raises_error(
        self, mock_connection: MagicMock
    ) -> None:
        # Arrange
        mock_connection.execute.return_value.mappings.return_value.fetchone.return_value = None
        registry = ContractRegistry(mock_connection)

        # Act & Assert
        with pytest.raises(ContractNotFoundError, match="No contract found"):
            registry.get("nonexistent_table")


class TestContractRegistryDelete:
    """Tests for contract deletion."""

    def test_delete_existing_contract_returns_true(
        self, mock_connection: MagicMock
    ) -> None:
        # Arrange
        mock_connection.execute.return_value.rowcount = 1
        registry = ContractRegistry(mock_connection)

        # Act
        result = registry.delete("orders")

        # Assert
        assert result is True
        assert mock_connection.commit.called

    def test_delete_nonexistent_contract_returns_false(
        self, mock_connection: MagicMock
    ) -> None:
        # Arrange
        mock_connection.execute.return_value.rowcount = 0
        registry = ContractRegistry(mock_connection)

        # Act
        result = registry.delete("nonexistent")

        # Assert
        assert result is False
