-- Raw Layer: Raw Customers Table
-- Append-only table storing all ingested customer records with metadata

CREATE TABLE IF NOT EXISTS raw.raw_customers (
    -- Source columns (preserved as-is from CSV)
    CustomerID VARCHAR,
    Name VARCHAR,
    HasLoan VARCHAR,  -- Kept as VARCHAR to preserve original format
    
    -- Metadata columns for lineage tracking
    _source_file VARCHAR NOT NULL,
    _file_hash VARCHAR NOT NULL,
    _row_number INTEGER NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    
    -- Composite key for identifying unique raw records
    PRIMARY KEY (_file_hash, _row_number)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_raw_customers_ingested ON raw.raw_customers(_ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_customers_customer_id ON raw.raw_customers(CustomerID);


