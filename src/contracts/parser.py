"""YAML data contract parser.

Why YAML for contracts: human-readable, supports comments for documentation,
and is the industry standard for data contracts (similar to dbt schema.yml).
Parsing into Pydantic models ensures strict validation at load time.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from src.models.contract import (
    ColumnDefinition,
    ColumnType,
    DataContract,
    FreshnessConfig,
    QualityRule,
    RuleType,
)

logger = logging.getLogger("data_governance")


class ContractParserError(Exception):
    """Raised when a contract file cannot be parsed or validated."""


class ContractParser:
    """Parses YAML contract files into validated DataContract models.

    Why a dedicated parser class: encapsulates YAML-specific parsing logic
    and error handling, keeping the DataContract model clean of file I/O concerns.
    """

    def parse_file(self, file_path: Path) -> DataContract:
        """Parse a single YAML contract file into a DataContract model.

        Args:
            file_path: Absolute or relative path to the YAML contract file.

        Returns:
            Validated DataContract instance.

        Raises:
            ContractParserError: If the file cannot be read or the content is invalid.
        """
        if not file_path.exists():
            raise ContractParserError(f"Contract file not found: {file_path}")

        if file_path.suffix not in (".yml", ".yaml"):
            raise ContractParserError(
                f"Contract file must be .yml or .yaml, got: {file_path.suffix}"
            )

        try:
            raw_content = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ContractParserError(f"Failed to read contract file {file_path}: {exc}") from exc

        return self.parse_yaml(raw_content, source=str(file_path))

    def parse_yaml(self, yaml_content: str, source: str = "<string>") -> DataContract:
        """Parse YAML string content into a DataContract model.

        Args:
            yaml_content: Raw YAML string.
            source: Identifier for error messages (e.g. file path).

        Returns:
            Validated DataContract instance.

        Raises:
            ContractParserError: If YAML is malformed or contract schema is invalid.
        """
        try:
            raw_data = yaml.safe_load(yaml_content)
        except yaml.YAMLError as exc:
            raise ContractParserError(f"Invalid YAML in {source}: {exc}") from exc

        if not isinstance(raw_data, dict):
            raise ContractParserError(
                f"Contract root must be a mapping, got {type(raw_data).__name__} in {source}"
            )

        return self._build_contract(raw_data, source)

    def parse_directory(self, directory: Path) -> list[DataContract]:
        """Parse all YAML contract files in a directory.

        Args:
            directory: Path to directory containing contract files.

        Returns:
            List of validated DataContract instances.

        Raises:
            ContractParserError: If the directory does not exist.
        """
        if not directory.is_dir():
            raise ContractParserError(f"Contracts directory not found: {directory}")

        contracts: list[DataContract] = []
        yaml_files = sorted(directory.glob("*.yml")) + sorted(directory.glob("*.yaml"))

        for file_path in yaml_files:
            try:
                contract = self.parse_file(file_path)
                contracts.append(contract)
                logger.info(
                    "contract_parsed",
                    extra={
                        "event": "contract_parsed",
                        "table": contract.table_name,
                        "source": str(file_path),
                    },
                )
            except ContractParserError:
                logger.exception(
                    "contract_parse_failed",
                    extra={
                        "event": "contract_parse_failed",
                        "source": str(file_path),
                    },
                )
                raise

        return contracts

    def _build_contract(self, raw_data: dict[str, Any], source: str) -> DataContract:
        """Transform raw YAML dict into a validated DataContract.

        Why a separate builder: isolates the mapping from YAML conventions
        (e.g. snake_case keys, string types) to Pydantic model fields.
        """
        columns = self._build_columns(raw_data.get("columns", []), source)
        quality_rules = self._build_quality_rules(raw_data.get("quality_rules", []), source)
        freshness = self._build_freshness(raw_data.get("freshness"), source)

        try:
            return DataContract(
                table_name=raw_data.get("table_name", ""),
                schema_name=raw_data.get("schema_name", "public"),
                owner=raw_data.get("owner", ""),
                description=raw_data.get("description"),
                version=raw_data.get("version", "1.0.0"),
                columns=columns,
                quality_rules=quality_rules,
                freshness=freshness,
                tags=raw_data.get("tags", []),
            )
        except ValidationError as exc:
            raise ContractParserError(
                f"Contract validation failed in {source}: {exc}"
            ) from exc

    def _build_columns(
        self, raw_columns: list[dict[str, Any]], source: str
    ) -> list[ColumnDefinition]:
        """Build column definitions from raw YAML data."""
        if not raw_columns:
            raise ContractParserError(f"Contract must define at least one column in {source}")

        columns: list[ColumnDefinition] = []
        for raw_col in raw_columns:
            try:
                col_type = ColumnType(raw_col.get("type", "string"))
            except ValueError as exc:
                raise ContractParserError(
                    f"Invalid column type '{raw_col.get('type')}' in {source}: {exc}"
                ) from exc

            try:
                column = ColumnDefinition(
                    name=raw_col.get("name", ""),
                    column_type=col_type,
                    nullable=raw_col.get("nullable", True),
                    description=raw_col.get("description"),
                    primary_key=raw_col.get("primary_key", False),
                )
            except ValidationError as exc:
                raise ContractParserError(
                    f"Invalid column definition in {source}: {exc}"
                ) from exc

            columns.append(column)
        return columns

    def _build_quality_rules(
        self, raw_rules: list[dict[str, Any]], source: str
    ) -> list[QualityRule]:
        """Build quality rules from raw YAML data."""
        rules: list[QualityRule] = []
        for raw_rule in raw_rules:
            try:
                rule_type = RuleType(raw_rule.get("rule_type", ""))
            except ValueError as exc:
                raise ContractParserError(
                    f"Invalid rule type '{raw_rule.get('rule_type')}' in {source}: {exc}"
                ) from exc

            try:
                rule = QualityRule(
                    rule_type=rule_type,
                    column=raw_rule.get("column"),
                    parameters=raw_rule.get("parameters", {}),
                    severity=raw_rule.get("severity", "error"),
                    description=raw_rule.get("description"),
                )
            except ValidationError as exc:
                raise ContractParserError(
                    f"Invalid quality rule in {source}: {exc}"
                ) from exc

            rules.append(rule)
        return rules

    def _build_freshness(
        self, raw_freshness: dict[str, Any] | None, source: str
    ) -> FreshnessConfig | None:
        """Build freshness config from raw YAML data."""
        if raw_freshness is None:
            return None

        try:
            return FreshnessConfig(
                timestamp_column=raw_freshness.get("timestamp_column", ""),
                max_delay_minutes=raw_freshness.get("max_delay_minutes", 60),
                check_interval_minutes=raw_freshness.get("check_interval_minutes", 60),
            )
        except ValidationError as exc:
            raise ContractParserError(
                f"Invalid freshness config in {source}: {exc}"
            ) from exc
