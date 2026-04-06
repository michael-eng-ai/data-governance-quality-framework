"""Contract registry for storing and retrieving data contracts.

Why a registry: provides a centralized catalog of all contracts with
persistence, enabling the governance dashboard to track coverage and
the quality engine to look up contracts by table name without re-parsing
YAML files on every run.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.contracts.parser import ContractParser
from src.models.contract import DataContract

logger = logging.getLogger("data_governance")


class ContractNotFoundError(Exception):
    """Raised when a requested contract does not exist in the registry."""


class ContractRegistry:
    """Persists data contracts to PostgreSQL for catalog and retrieval.

    Why DB-backed registry: file-based lookups are fragile in containerized
    environments where the filesystem may not be shared. A DB registry also
    enables versioning, auditing, and API-based contract management.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def register(self, contract: DataContract) -> DataContract:
        """Insert or update a data contract in the registry.

        Args:
            contract: Validated DataContract to persist.

        Returns:
            The contract with updated timestamps.
        """
        now = datetime.now(timezone.utc)
        contract_json = contract.model_dump_json()

        existing = self._find_by_table(contract.table_name, contract.schema_name)

        if existing:
            query = text(
                "UPDATE data_contracts"
                " SET contract_data = :contract_data,"
                "     version = :version,"
                "     owner = :owner,"
                "     updated_at = :updated_at"
                " WHERE table_name = :table_name"
                " AND schema_name = :schema_name"
            )
            self._connection.execute(
                query,
                {
                    "contract_data": contract_json,
                    "version": contract.version,
                    "owner": contract.owner,
                    "updated_at": now,
                    "table_name": contract.table_name,
                    "schema_name": contract.schema_name,
                },
            )
            contract.updated_at = now
            logger.info(
                "contract_updated",
                extra={
                    "event": "contract_updated",
                    "table": contract.table_name,
                    "version": contract.version,
                },
            )
        else:
            query = text(
                "INSERT INTO data_contracts"
                " (table_name, schema_name, owner, version, contract_data, created_at, updated_at)"
                " VALUES (:table_name, :schema_name, :owner, :version, :contract_data,"
                "         :created_at, :updated_at)"
            )
            self._connection.execute(
                query,
                {
                    "table_name": contract.table_name,
                    "schema_name": contract.schema_name,
                    "owner": contract.owner,
                    "version": contract.version,
                    "contract_data": contract_json,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            contract.created_at = now
            contract.updated_at = now
            logger.info(
                "contract_registered",
                extra={
                    "event": "contract_registered",
                    "table": contract.table_name,
                    "version": contract.version,
                },
            )

        self._connection.commit()
        return contract

    def get(self, table_name: str, schema_name: str = "public") -> DataContract:
        """Retrieve a contract by table name.

        Args:
            table_name: Target table name.
            schema_name: Target schema name.

        Returns:
            The matching DataContract.

        Raises:
            ContractNotFoundError: If no contract exists for the table.
        """
        row = self._find_by_table(table_name, schema_name)
        if not row:
            raise ContractNotFoundError(
                f"No contract found for {schema_name}.{table_name}"
            )

        contract_data: dict[str, Any] = json.loads(row["contract_data"])
        return DataContract(**contract_data)

    def list_all(self) -> list[DataContract]:
        """Retrieve all registered contracts.

        Returns:
            List of all DataContract instances in the registry.
        """
        query = text(
            "SELECT contract_data FROM data_contracts ORDER BY table_name"
        )
        rows = self._connection.execute(query).fetchall()

        contracts: list[DataContract] = []
        parser = ContractParser()
        for row in rows:
            contract_data: dict[str, Any] = json.loads(row[0])
            contracts.append(DataContract(**contract_data))

        return contracts

    def delete(self, table_name: str, schema_name: str = "public") -> bool:
        """Remove a contract from the registry.

        Args:
            table_name: Target table name.
            schema_name: Target schema name.

        Returns:
            True if a contract was deleted, False if not found.
        """
        query = text(
            "DELETE FROM data_contracts"
            " WHERE table_name = :table_name"
            " AND schema_name = :schema_name"
        )
        result = self._connection.execute(
            query,
            {"table_name": table_name, "schema_name": schema_name},
        )
        self._connection.commit()
        deleted = result.rowcount > 0

        if deleted:
            logger.info(
                "contract_deleted",
                extra={
                    "event": "contract_deleted",
                    "table": table_name,
                    "schema": schema_name,
                },
            )

        return deleted

    def count(self) -> int:
        """Return the total number of registered contracts."""
        query = text("SELECT COUNT(*) FROM data_contracts")
        row = self._connection.execute(query).fetchone()
        return row[0] if row else 0

    def _find_by_table(
        self, table_name: str, schema_name: str
    ) -> dict[str, Any] | None:
        """Look up a contract row by table and schema name."""
        query = text(
            "SELECT table_name, schema_name, owner, version, contract_data,"
            "       created_at, updated_at"
            " FROM data_contracts"
            " WHERE table_name = :table_name"
            " AND schema_name = :schema_name"
        )
        row = self._connection.execute(
            query,
            {"table_name": table_name, "schema_name": schema_name},
        ).mappings().fetchone()

        if not row:
            return None
        return dict(row)
