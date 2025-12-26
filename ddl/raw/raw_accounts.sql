-- Raw Layer: Raw Accounts Table
-- Append-only table storing all ingested account records with metadata

CREATE TABLE IF NOT EXISTS raw.raw_accounts (
    -- Source columns (preserved as-is from CSV)
    AccountID VARCHAR,
    CustomerID VARCHAR,
    Balance VARCHAR,  -- Kept as VARCHAR to preserve original format
    AccountType VARCHAR,
    
    -- Metadata columns for lineage tracking
    _source_file VARCHAR NOT NULL,
    _file_hash VARCHAR NOT NULL,
    _row_number INTEGER NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    
    -- Composite key for identifying unique raw records
    PRIMARY KEY (_file_hash, _row_number)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_raw_accounts_ingested ON raw.raw_accounts(_ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_accounts_account_id ON raw.raw_accounts(AccountID);


