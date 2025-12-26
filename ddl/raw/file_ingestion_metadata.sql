-- Raw Layer: File Ingestion Metadata Table
-- Tracks all files ingested into the raw layer

CREATE TABLE IF NOT EXISTS raw.file_ingestion_metadata (
    file_id VARCHAR PRIMARY KEY,
    file_path VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL,
    file_type VARCHAR NOT NULL,
    file_hash VARCHAR NOT NULL,
    file_size_bytes BIGINT,
    row_count INTEGER,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_status VARCHAR DEFAULT 'pending',
    error_message VARCHAR,
    UNIQUE(file_hash)
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_file_metadata_hash ON raw.file_ingestion_metadata(file_hash);
CREATE INDEX IF NOT EXISTS idx_file_metadata_status ON raw.file_ingestion_metadata(processing_status);


