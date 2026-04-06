-- Data Governance Framework - Initial Schema
-- Why separate tables: each concern (contracts, results, metrics) has
-- different access patterns and lifecycle. Contracts are modified rarely,
-- results are append-heavy, and metrics are time-series data.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Data Contracts Registry
-- Stores the canonical contract definition for each table.
CREATE TABLE IF NOT EXISTS data_contracts (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    table_name      VARCHAR(255) NOT NULL,
    schema_name     VARCHAR(255) NOT NULL DEFAULT 'public',
    owner           VARCHAR(255) NOT NULL,
    version         VARCHAR(20) NOT NULL DEFAULT '1.0.0',
    contract_data   JSONB NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (table_name, schema_name)
);

CREATE INDEX IF NOT EXISTS idx_contracts_table_schema
    ON data_contracts (table_name, schema_name);
CREATE INDEX IF NOT EXISTS idx_contracts_owner
    ON data_contracts (owner);

-- Quality Check Results
-- Append-only log of every quality check execution.
CREATE TABLE IF NOT EXISTS quality_results (
    id                  UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    run_id              UUID NOT NULL,
    table_name          VARCHAR(255) NOT NULL,
    schema_name         VARCHAR(255) NOT NULL DEFAULT 'public',
    contract_version    VARCHAR(20) NOT NULL,
    overall_status      VARCHAR(20) NOT NULL,
    total_checks        INTEGER NOT NULL DEFAULT 0,
    passed_checks       INTEGER NOT NULL DEFAULT 0,
    failed_checks       INTEGER NOT NULL DEFAULT 0,
    warning_checks      INTEGER NOT NULL DEFAULT 0,
    duration_seconds    DOUBLE PRECISION DEFAULT 0.0,
    result_data         JSONB NOT NULL,
    executed_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_results_table_schema
    ON quality_results (table_name, schema_name);
CREATE INDEX IF NOT EXISTS idx_results_executed_at
    ON quality_results (executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_results_status
    ON quality_results (overall_status);
CREATE INDEX IF NOT EXISTS idx_results_run_id
    ON quality_results (run_id);

-- Governance Metrics Snapshots
-- Time-series data for governance dashboard trends.
CREATE TABLE IF NOT EXISTS governance_metrics (
    id                          UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    metric_id                   UUID NOT NULL,
    contract_coverage_pct       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    quality_pass_rate_pct       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    sla_compliance_pct          DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    total_tables                INTEGER NOT NULL DEFAULT 0,
    tables_with_contracts       INTEGER NOT NULL DEFAULT 0,
    total_checks_run            INTEGER NOT NULL DEFAULT 0,
    total_checks_passed         INTEGER NOT NULL DEFAULT 0,
    tables_within_sla           INTEGER NOT NULL DEFAULT 0,
    tables_with_freshness_sla   INTEGER NOT NULL DEFAULT 0,
    captured_at                 TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_captured_at
    ON governance_metrics (captured_at DESC);

-- Sample data tables for demonstration and testing
CREATE TABLE IF NOT EXISTS orders (
    order_id        UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    customer_id     UUID NOT NULL,
    order_date      DATE NOT NULL,
    total_amount    DECIMAL(12,2) NOT NULL CHECK (total_amount >= 0),
    status          VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id     UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    email           VARCHAR(255) UNIQUE NOT NULL,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    country         VARCHAR(100),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert sample data for demonstration
INSERT INTO customers (customer_id, email, first_name, last_name, country, created_at, updated_at)
VALUES
    ('a1b2c3d4-e5f6-4a5b-8c7d-9e0f1a2b3c4d', 'alice@example.com', 'Alice', 'Johnson', 'US', NOW(), NOW()),
    ('b2c3d4e5-f6a7-4b5c-9d8e-0f1a2b3c4d5e', 'bob@example.com', 'Bob', 'Smith', 'UK', NOW(), NOW()),
    ('c3d4e5f6-a7b8-4c5d-0e9f-1a2b3c4d5e6f', 'carol@example.com', 'Carol', 'Williams', 'BR', NOW(), NOW())
ON CONFLICT (email) DO NOTHING;

INSERT INTO orders (order_id, customer_id, order_date, total_amount, status, created_at, updated_at)
VALUES
    ('d4e5f6a7-b8c9-4d5e-1f0a-2b3c4d5e6f7a', 'a1b2c3d4-e5f6-4a5b-8c7d-9e0f1a2b3c4d', '2024-01-15', 150.00, 'completed', NOW(), NOW()),
    ('e5f6a7b8-c9d0-4e5f-2a1b-3c4d5e6f7a8b', 'b2c3d4e5-f6a7-4b5c-9d8e-0f1a2b3c4d5e', '2024-01-16', 250.50, 'completed', NOW(), NOW()),
    ('f6a7b8c9-d0e1-4f5a-3b2c-4d5e6f7a8b9c', 'c3d4e5f6-a7b8-4c5d-0e9f-1a2b3c4d5e6f', '2024-01-17', 99.99, 'pending', NOW(), NOW())
ON CONFLICT DO NOTHING;
