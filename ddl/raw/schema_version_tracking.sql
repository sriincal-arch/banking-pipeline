-- Raw Layer: Schema Version Tracking Table
-- Tracks schema changes detected at ingestion time

CREATE TABLE IF NOT EXISTS raw.schema_version_tracking (
    schema_id VARCHAR PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    schema_version INTEGER NOT NULL,
    column_definitions JSON NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    previous_version_id VARCHAR,
    change_description VARCHAR
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_schema_version_table ON raw.schema_version_tracking(table_name);


