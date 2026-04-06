# Data Governance & Quality Framework

Automated data quality gates across the entire pipeline lifecycle with contract enforcement, freshness SLAs, and governance checks.

## Architecture

```
YAML Contracts --> Contract Parser --> Contract Registry (PostgreSQL)
                                           |
                                    Quality Engine
                                    /      |      \
                          Great Expectations  Soda   Freshness Checker
                                    \      |      /
                                  Validation Results
                                     |          |
                              Governance     Alert
                              Dashboard      Manager
                                  |
                              Metrics API
```

### Core Components

- **Data Contracts**: YAML-defined schemas with column types, quality rules, and freshness SLAs
- **Contract Registry**: PostgreSQL-backed catalog for contract storage and retrieval
- **Quality Engine**: Orchestrates checks across Great Expectations, Soda, and custom validators
- **Freshness Checker**: Monitors table update timestamps against SLA thresholds
- **Governance Dashboard**: Tracks contract coverage, quality pass rates, and SLA compliance
- **Alert Manager**: Sends webhook/email notifications on quality failures and SLA breaches
- **REST API**: FastAPI endpoints for contract management, quality triggers, and metrics

## Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.11+ |
| API | FastAPI |
| Database | PostgreSQL 16 |
| Quality | Great Expectations, Soda Core |
| Orchestration | Apache Airflow |
| Transformation | dbt |
| Container | Docker, Docker Compose |
| CI/CD | GitHub Actions |

## Quick Start

### Docker Compose

```bash
# Start all services
docker compose up -d

# Verify health
curl http://localhost:8000/health

# Register a contract
curl -X POST http://localhost:8000/contracts \
  -H "Content-Type: application/json" \
  -d '{"yaml_content": "$(cat contracts/orders.yml)"}'

# Run quality checks
curl -X POST http://localhost:8000/quality/run/orders

# View governance metrics
curl http://localhost:8000/governance/metrics
```

### Local Development

```bash
# Install dependencies
pip install -e ".[dev]"

# Set environment variables
cp .env.example .env
# Edit .env with your database credentials

# Run tests
pytest

# Start the API server
uvicorn src.api.app:create_app --factory --reload

# Lint and type check
ruff check src/ tests/
mypy src/ --ignore-missing-imports
```

## Data Contracts

Contracts are defined in YAML and specify:

```yaml
table_name: orders
schema_name: public
owner: data-engineering
version: "1.0.0"

columns:
  - name: order_id
    type: uuid
    nullable: false
    primary_key: true

quality_rules:
  - rule_type: accepted_values
    column: status
    parameters:
      values: [pending, completed, cancelled]

freshness:
  timestamp_column: updated_at
  max_delay_minutes: 120
```

### Supported Rule Types

| Rule Type | Description |
|-----------|-------------|
| `not_null` | Column must not contain null values |
| `unique` | Column values must be unique |
| `accepted_values` | Column values must be in a defined set |
| `min_value` | Column values must be >= threshold |
| `max_value` | Column values must be <= threshold |
| `regex_match` | Column values must match a regex pattern |
| `custom_sql` | Custom SQL-based validation |
| `row_count_min` | Table must have minimum row count |
| `referential_integrity` | Foreign key references must exist |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Application health check |
| POST | `/contracts` | Register a new contract |
| GET | `/contracts` | List all contracts |
| GET | `/contracts/{table}` | Get contract detail |
| DELETE | `/contracts/{table}` | Remove a contract |
| POST | `/quality/run/{table}` | Run checks for a table |
| POST | `/quality/run-all` | Run checks for all tables |
| GET | `/quality/results` | List recent results |
| GET | `/quality/results/{id}` | Get result detail |
| GET | `/quality/failing` | List failing tables |
| GET | `/governance/metrics` | Current governance metrics |
| POST | `/governance/metrics/capture` | Capture new snapshot |
| GET | `/governance/trends` | Metrics over time |

## dbt Integration

The framework includes a dbt project with:

- **Staging models**: Deduplication and standardization
- **Mart models**: Pre-computed aggregations
- **Custom tests**: Contract-aligned data quality tests
- **Macros**: Reusable freshness SLA and schema validation

```bash
cd dbt_project
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Airflow DAG

The `governance_pipeline_dag.py` runs every 2 hours:

1. **Sync Contracts** - Load YAML contracts into the registry
2. **Run Quality Checks** - Execute all validation engines
3. **Capture Metrics** - Snapshot governance health
4. **Generate Report** - Create summary quality report

## Project Structure

```
src/
  config.py              # Pydantic settings from env vars
  models/                # Domain models (contracts, quality, governance)
  contracts/             # YAML parser, validator, registry
  quality/               # GE runner, Soda runner, freshness checker
  governance/            # Dashboard metrics, reporter, alerts
  api/                   # FastAPI app and route handlers
  db/                    # Database session and migrations
contracts/               # YAML data contract definitions
dbt_project/             # dbt models, tests, and macros
dags/                    # Airflow DAG definitions
tests/                   # pytest test suite
```

## Testing

```bash
# Run all tests with coverage
pytest --cov=src --cov-report=term-missing

# Run specific test modules
pytest tests/test_contracts/
pytest tests/test_quality/
pytest tests/test_api/
```

## License

MIT
