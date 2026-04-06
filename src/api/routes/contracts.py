"""Data contract CRUD endpoints.

Why REST for contract management: enables programmatic contract registration
from CI/CD pipelines and dbt post-hooks, not just manual YAML file management.
The API serves as the primary interface for contract lifecycle operations.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.contracts.parser import ContractParser, ContractParserError
from src.contracts.registry import ContractNotFoundError, ContractRegistry
from src.db.session import get_connection
from src.models.contract import DataContract

logger = logging.getLogger("data_governance")

router = APIRouter()


class ContractCreateRequest(BaseModel):
    """Request body for creating a contract via YAML content."""

    yaml_content: str = Field(
        ...,
        description="YAML content of the data contract",
        min_length=10,
    )


class ContractResponse(BaseModel):
    """Standardized contract response."""

    table_name: str
    schema_name: str
    owner: str
    version: str
    description: str | None
    column_count: int
    rule_count: int
    has_freshness_sla: bool
    tags: list[str]


def _contract_to_response(contract: DataContract) -> ContractResponse:
    """Map a DataContract to the API response format."""
    return ContractResponse(
        table_name=contract.table_name,
        schema_name=contract.schema_name,
        owner=contract.owner,
        version=contract.version,
        description=contract.description,
        column_count=len(contract.columns),
        rule_count=len(contract.quality_rules),
        has_freshness_sla=contract.freshness is not None,
        tags=contract.tags,
    )


@router.post("", status_code=201)
def create_contract(request: ContractCreateRequest) -> dict[str, Any]:
    """Register a new data contract from YAML content.

    Why accept YAML via API: allows CI/CD pipelines to register contracts
    as part of the deployment process without requiring file system access.
    """
    parser = ContractParser()

    try:
        contract = parser.parse_yaml(request.yaml_content, source="api_request")
    except ContractParserError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    with get_connection() as conn:
        registry = ContractRegistry(conn)
        registered = registry.register(contract)

    return {
        "status": "registered",
        "contract": _contract_to_response(registered).model_dump(),
    }


@router.get("")
def list_contracts() -> dict[str, Any]:
    """List all registered data contracts.

    Why return summary rather than full contracts: reduces payload size
    for dashboard listings. Full detail is available via the detail endpoint.
    """
    with get_connection() as conn:
        registry = ContractRegistry(conn)
        contracts = registry.list_all()

    return {
        "total": len(contracts),
        "contracts": [_contract_to_response(c).model_dump() for c in contracts],
    }


@router.get("/{table_name}")
def get_contract(table_name: str, schema_name: str = "public") -> dict[str, Any]:
    """Get full contract detail for a specific table.

    Why separate from list: the full contract includes all columns,
    rules, and freshness config which is too verbose for list views.
    """
    with get_connection() as conn:
        registry = ContractRegistry(conn)
        try:
            contract = registry.get(table_name, schema_name)
        except ContractNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {
        "contract": contract.model_dump(mode="json"),
    }


@router.delete("/{table_name}", status_code=200)
def delete_contract(table_name: str, schema_name: str = "public") -> dict[str, Any]:
    """Remove a data contract from the registry.

    Why allow deletion: contracts may become obsolete when tables are
    deprecated. Keeping stale contracts inflates coverage metrics.
    """
    with get_connection() as conn:
        registry = ContractRegistry(conn)
        deleted = registry.delete(table_name, schema_name)

    if not deleted:
        raise HTTPException(
            status_code=404,
            detail=f"No contract found for {schema_name}.{table_name}",
        )

    return {
        "status": "deleted",
        "table_name": table_name,
        "schema_name": schema_name,
    }
