-- Database initialization script for data pipeline
-- This script sets up the basic schema and tables needed for the data pipeline

-- Create schemas
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS metadata;
CREATE SCHEMA IF NOT EXISTS data_quality;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Set search path
SET search_path = public, metadata, data_quality, monitoring;

-- Create data lineage tracking table
CREATE TABLE IF NOT EXISTS metadata.data_lineage (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    transformation_type VARCHAR(100),
    pipeline_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

-- Create data quality metrics table
CREATE TABLE IF NOT EXISTS data_quality.quality_metrics (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(10, 4),
    threshold_value DECIMAL(10, 4),
    status VARCHAR(20) DEFAULT 'PASS',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);

-- Create pipeline execution log
CREATE TABLE IF NOT EXISTS monitoring.pipeline_executions (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB
);

-- Create data catalog table
CREATE TABLE IF NOT EXISTS metadata.data_catalog (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL UNIQUE,
    schema_name VARCHAR(100) DEFAULT 'public',
    description TEXT,
    owner VARCHAR(100),
    tags TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    column_info JSONB,
    table_stats JSONB
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_lineage_source ON metadata.data_lineage(source_table);
CREATE INDEX IF NOT EXISTS idx_lineage_target ON metadata.data_lineage(target_table);
CREATE INDEX IF NOT EXISTS idx_quality_table ON data_quality.quality_metrics(table_name);
CREATE INDEX IF NOT EXISTS idx_quality_created ON data_quality.quality_metrics(created_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_name ON monitoring.pipeline_executions(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_status ON monitoring.pipeline_executions(status);

-- Insert some sample data for testing
INSERT INTO metadata.data_catalog (table_name, description, owner, tags) VALUES 
('sample_data', 'Sample dataset for testing pipeline functionality', 'data_engineer', ARRAY['test', 'sample']),
('employees', 'Employee data with demographics and salary information', 'hr_team', ARRAY['hr', 'sensitive']),
('sales', 'Sales transaction data', 'sales_team', ARRAY['sales', 'revenue'])
ON CONFLICT (table_name) DO NOTHING;