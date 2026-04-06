"""Data contract domain models.

Why Pydantic models for contracts: enforces schema validation at parse time
so malformed contracts fail fast with clear error messages rather than
causing silent data quality issues downstream.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class ColumnType(StrEnum):
    """Supported column types mapped to PostgreSQL types.

    Why an enum: prevents typos in contract definitions and enables
    compile-time validation of type mappings.
    """

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    DECIMAL = "decimal"
    TEXT = "text"
    UUID = "uuid"


class RuleType(StrEnum):
    """Quality rule types that map to Great Expectations and Soda checks."""

    NOT_NULL = "not_null"
    UNIQUE = "unique"
    ACCEPTED_VALUES = "accepted_values"
    MIN_VALUE = "min_value"
    MAX_VALUE = "max_value"
    REGEX_MATCH = "regex_match"
    CUSTOM_SQL = "custom_sql"
    ROW_COUNT_MIN = "row_count_min"
    REFERENTIAL_INTEGRITY = "referential_integrity"


class QualityRule(BaseModel):
    """A single quality rule within a data contract.

    Why per-column rules: enables granular quality gates that can be
    independently enabled/disabled without affecting the entire contract.
    """

    rule_type: RuleType
    column: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    severity: str = Field(default="error", pattern=r"^(error|warning)$")
    description: str | None = None


class FreshnessConfig(BaseModel):
    """Freshness SLA configuration for a data contract.

    Why separate from column definitions: freshness is a table-level
    concern that may reference any timestamp column, not a per-column rule.
    """

    timestamp_column: str
    max_delay_minutes: int = Field(ge=1, le=10080)
    check_interval_minutes: int = Field(default=60, ge=1, le=1440)


class ColumnDefinition(BaseModel):
    """Schema definition for a single column in a data contract.

    Why explicit nullable flag: distinguishes between 'column may have nulls'
    and 'column should never have nulls' which are different quality concerns.
    """

    name: str = Field(min_length=1, max_length=255)
    column_type: ColumnType
    nullable: bool = True
    description: str | None = None
    primary_key: bool = False

    @field_validator("name")
    @classmethod
    def validate_column_name(cls, value: str) -> str:
        """Reject column names that could cause SQL injection or ambiguity."""
        if not value.replace("_", "").isalnum():
            raise ValueError(
                f"Column name '{value}' contains invalid characters. "
                "Only alphanumeric and underscore allowed."
            )
        return value.lower()


class DataContract(BaseModel):
    """Complete data contract defining schema, quality rules, and SLAs.

    Why a unified contract: keeps all governance metadata for a table in one
    place, making it easy to audit coverage and detect configuration drift.
    """

    table_name: str = Field(min_length=1, max_length=255)
    schema_name: str = Field(default="public", min_length=1, max_length=255)
    owner: str = Field(min_length=1, max_length=255)
    description: str | None = None
    version: str = Field(default="1.0.0", pattern=r"^\d+\.\d+\.\d+$")
    columns: list[ColumnDefinition] = Field(min_length=1)
    quality_rules: list[QualityRule] = Field(default_factory=list)
    freshness: FreshnessConfig | None = None
    tags: list[str] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, value: str) -> str:
        """Reject table names that could cause SQL injection."""
        if not value.replace("_", "").isalnum():
            raise ValueError(
                f"Table name '{value}' contains invalid characters. "
                "Only alphanumeric and underscore allowed."
            )
        return value.lower()

    def get_required_columns(self) -> list[ColumnDefinition]:
        """Return columns that must not contain null values."""
        return [col for col in self.columns if not col.nullable]

    def get_primary_key_columns(self) -> list[ColumnDefinition]:
        """Return columns marked as primary keys."""
        return [col for col in self.columns if col.primary_key]
