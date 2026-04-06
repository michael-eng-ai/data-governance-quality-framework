"""Tests for FastAPI routes.

Why test routes via TestClient: validates request/response serialization,
HTTP status codes, and error handling at the API boundary. These are the
tests that catch breaking changes visible to API consumers.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.api.app import create_app
from src.models.contract import DataContract


@pytest.fixture
def client() -> TestClient:
    """Create a test client with mocked database."""
    app = create_app()
    return TestClient(app)


class TestHealthEndpoint:
    """Tests for the health check endpoint."""

    def test_health_returns_200(self, client: TestClient) -> None:
        # Arrange & Act
        with patch("src.api.routes.health.check_database_health", return_value=True):
            response = client.get("/health")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["database"] is True

    def test_health_degraded_when_db_down(self, client: TestClient) -> None:
        # Arrange & Act
        with patch("src.api.routes.health.check_database_health", return_value=False):
            response = client.get("/health")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "degraded"
        assert data["database"] is False


class TestContractsEndpoints:
    """Tests for contract CRUD endpoints."""

    def test_create_contract_returns_201(self, client: TestClient) -> None:
        # Arrange
        yaml_content = """
table_name: test_table
schema_name: public
owner: test-team
columns:
  - name: id
    type: integer
    nullable: false
    primary_key: true
  - name: name
    type: string
    nullable: false
"""
        with patch("src.api.routes.contracts.get_connection") as mock_conn:
            mock_ctx = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=mock_ctx)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_ctx.execute.return_value.mappings.return_value.fetchone.return_value = None

            # Act
            response = client.post(
                "/contracts",
                json={"yaml_content": yaml_content},
            )

        # Assert
        assert response.status_code == 201
        data = response.json()
        assert data["status"] == "registered"
        assert data["contract"]["table_name"] == "test_table"

    def test_create_contract_invalid_yaml_returns_422(self, client: TestClient) -> None:
        # Arrange & Act
        response = client.post(
            "/contracts",
            json={"yaml_content": "{{invalid yaml"},
        )

        # Assert
        assert response.status_code == 422

    def test_list_contracts_returns_empty_list(self, client: TestClient) -> None:
        # Arrange
        with patch("src.api.routes.contracts.get_connection") as mock_conn:
            mock_ctx = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=mock_ctx)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_ctx.execute.return_value.fetchall.return_value = []

            # Act
            response = client.get("/contracts")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0

    def test_get_nonexistent_contract_returns_404(self, client: TestClient) -> None:
        # Arrange
        with patch("src.api.routes.contracts.get_connection") as mock_conn:
            mock_ctx = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=mock_ctx)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_ctx.execute.return_value.mappings.return_value.fetchone.return_value = None

            # Act
            response = client.get("/contracts/nonexistent")

        # Assert
        assert response.status_code == 404


class TestQualityEndpoints:
    """Tests for quality check endpoints."""

    def test_list_results_returns_200(self, client: TestClient) -> None:
        # Arrange
        with patch("src.api.routes.quality.get_connection") as mock_conn:
            mock_ctx = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=mock_ctx)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_ctx.execute.return_value.mappings.return_value.fetchall.return_value = []

            # Act
            response = client.get("/quality/results")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert "results" in data

    def test_get_nonexistent_result_returns_404(self, client: TestClient) -> None:
        # Arrange
        with patch("src.api.routes.quality.get_connection") as mock_conn:
            mock_ctx = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=mock_ctx)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_ctx.execute.return_value.mappings.return_value.fetchone.return_value = None

            # Act
            response = client.get("/quality/results/00000000-0000-0000-0000-000000000000")

        # Assert
        assert response.status_code == 404


class TestGovernanceEndpoints:
    """Tests for governance dashboard endpoints."""

    def test_get_metrics_returns_200(self, client: TestClient) -> None:
        # Arrange
        mock_metrics = {
            "metric_id": "test-id",
            "contract_coverage_pct": 75.0,
            "quality_pass_rate_pct": 90.0,
            "sla_compliance_pct": 100.0,
            "total_tables": 10,
            "tables_with_contracts": 8,
            "total_checks_run": 50,
            "total_checks_passed": 45,
            "tables_within_sla": 8,
            "tables_with_freshness_sla": 8,
            "captured_at": "2024-01-01T00:00:00+00:00",
        }

        with patch("src.api.routes.governance.get_connection") as mock_conn:
            mock_ctx = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=mock_ctx)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_ctx.execute.return_value.mappings.return_value.fetchone.return_value = mock_metrics

            # Act
            response = client.get("/governance/metrics")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert "metrics" in data

    def test_get_trends_returns_200(self, client: TestClient) -> None:
        # Arrange
        with patch("src.api.routes.governance.get_connection") as mock_conn:
            mock_ctx = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=mock_ctx)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_ctx.execute.return_value.mappings.return_value.fetchall.return_value = []
            # For capture_snapshot fallback
            mock_ctx.execute.return_value.fetchone.return_value = MagicMock(
                __getitem__=lambda self, idx: 0
            )

            # Act
            response = client.get("/governance/trends?days=7")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert "snapshots" in data
