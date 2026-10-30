-- ==============================================================================
-- 01_init_metadata_schema.sql
-- Operational metadata and lakehouse governance schema
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS metadata;

-- Pipeline Runs
CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    pipeline_name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL, -- RUNNING, SUCCESS, FAILED, RETRIED
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC(10, 2),
    error_message TEXT,
    config_payload JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Task Runs
CREATE TABLE IF NOT EXISTS metadata.task_runs (
    task_run_id VARCHAR(64) PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL REFERENCES metadata.pipeline_runs(run_id) ON DELETE CASCADE,
    task_name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC(10, 2),
    rows_in INTEGER DEFAULT 0,
    rows_out INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Ingestion Batches
CREATE TABLE IF NOT EXISTS metadata.ingestion_batches (
    batch_id VARCHAR(64) PRIMARY KEY,
    source_name VARCHAR(64) NOT NULL,
    entity VARCHAR(64) NOT NULL,
    batch_type VARCHAR(32) NOT NULL, -- INITIAL, INCREMENTAL, BACKFILL
    record_count INTEGER DEFAULT 0,
    file_count INTEGER DEFAULT 0,
    ingestion_status VARCHAR(32) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE
);

-- Source File Manifests (File-level deduplication)
CREATE TABLE IF NOT EXISTS metadata.source_file_manifests (
    file_id VARCHAR(64) PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    file_name VARCHAR(256) NOT NULL,
    file_path TEXT NOT NULL,
    file_checksum_sha256 VARCHAR(64) NOT NULL UNIQUE,
    file_size_bytes BIGINT NOT NULL,
    record_count INTEGER DEFAULT 0,
    status VARCHAR(32) NOT NULL, -- PROCESSED, DUPLICATE_SKIPPED, FAILED
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Watermarks (Stateful Incremental Ingestion)
CREATE TABLE IF NOT EXISTS metadata.watermarks (
    entity_name VARCHAR(64) PRIMARY KEY,
    watermark_type VARCHAR(32) NOT NULL DEFAULT 'TIMESTAMP',
    watermark_value VARCHAR(128) NOT NULL,
    last_batch_id VARCHAR(64) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality Results
CREATE TABLE IF NOT EXISTS metadata.data_quality_results (
    test_id VARCHAR(64) PRIMARY KEY,
    run_id VARCHAR(64),
    layer VARCHAR(16) NOT NULL, -- BRONZE, SILVER, GOLD
    entity VARCHAR(64) NOT NULL,
    rule_name VARCHAR(128) NOT NULL,
    rule_type VARCHAR(64) NOT NULL, -- NULL_CHECK, UNIQUENESS, RECONCILIATION, RANGE, FORMAT
    status VARCHAR(16) NOT NULL, -- PASS, FAIL, WARN
    records_tested INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    details TEXT,
    tested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Quarantined Records
CREATE TABLE IF NOT EXISTS metadata.quarantined_records (
    quarantine_id VARCHAR(64) PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    entity VARCHAR(64) NOT NULL,
    source_record_id VARCHAR(128),
    raw_payload TEXT NOT NULL,
    error_category VARCHAR(64) NOT NULL, -- SCHEMA_VIOLATION, MISSING_REQUIRED_KEY, INVALID_VALUE, DUPLICATE
    error_message TEXT NOT NULL,
    quarantined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Schema Evolution Events
CREATE TABLE IF NOT EXISTS metadata.schema_change_events (
    event_id VARCHAR(64) PRIMARY KEY,
    entity VARCHAR(64) NOT NULL,
    layer VARCHAR(16) NOT NULL,
    change_type VARCHAR(64) NOT NULL, -- NEW_OPTIONAL_COLUMN, MISSING_REQUIRED_COLUMN, TYPE_MISMATCH
    previous_schema TEXT,
    current_schema TEXT,
    is_compatible BOOLEAN NOT NULL DEFAULT TRUE,
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Backfill Runs
CREATE TABLE IF NOT EXISTS metadata.backfill_runs (
    backfill_id VARCHAR(64) PRIMARY KEY,
    entity VARCHAR(64) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status VARCHAR(32) NOT NULL, -- IN_PROGRESS, COMPLETED, FAILED
    processed_partitions TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE
);

-- Published Dataset Versions
CREATE TABLE IF NOT EXISTS metadata.published_dataset_versions (
    version_id VARCHAR(64) PRIMARY KEY,
    dataset_name VARCHAR(64) NOT NULL,
    layer VARCHAR(16) NOT NULL,
    version_tag VARCHAR(32) NOT NULL,
    row_count INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_task_runs_run_id ON metadata.task_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_dq_layer_entity ON metadata.data_quality_results(layer, entity);
CREATE INDEX IF NOT EXISTS idx_quarantine_batch_entity ON metadata.quarantined_records(batch_id, entity);
CREATE INDEX IF NOT EXISTS idx_manifest_checksum ON metadata.source_file_manifests(file_checksum_sha256);
