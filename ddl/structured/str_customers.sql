-- Structured Layer: Structured Customers Table
-- Cleansed, standardized, and deduplicated customer records

CREATE TABLE IF NOT EXISTS structured.str_customers (
    -- Primary key
    customer_id VARCHAR PRIMARY KEY,
    
    -- Business columns (cleansed and typed)
    customer_name VARCHAR NOT NULL,
    hasloan BOOLEAN NOT NULL,
    
    -- Audit columns
    created_date TIMESTAMP NOT NULL,
    modified_date TIMESTAMP NOT NULL,
    metadata_json JSON,
    
    -- Processing tracking
    _last_processed_at TIMESTAMP NOT NULL
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_str_customers_name ON structured.str_customers(customer_name);
CREATE INDEX IF NOT EXISTS idx_str_customers_loan ON structured.str_customers(hasloan);
CREATE INDEX IF NOT EXISTS idx_str_customers_processed ON structured.str_customers(_last_processed_at);


