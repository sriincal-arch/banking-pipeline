-- Curated Layer: Process Metadata Table
-- Tracks ETL run information and processing statistics

CREATE TABLE IF NOT EXISTS curated.process_metadata (
    run_id VARCHAR PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_tables JSON,
    target_table VARCHAR,
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    status VARCHAR,
    duration_seconds FLOAT,
    error_message VARCHAR
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_process_metadata_timestamp ON curated.process_metadata(run_timestamp);
CREATE INDEX IF NOT EXISTS idx_process_metadata_target ON curated.process_metadata(target_table);


