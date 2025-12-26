-- Structured Layer: Structured Accounts Table
-- Cleansed, standardized, and deduplicated account records

CREATE TABLE IF NOT EXISTS structured.str_accounts (
    -- Primary key
    account_id VARCHAR PRIMARY KEY,
    
    -- Business columns (cleansed and typed)
    account_type VARCHAR NOT NULL,
    original_balance DECIMAL(15, 2) NOT NULL,
    customer_id VARCHAR NOT NULL,
    
    -- Audit columns
    created_date TIMESTAMP NOT NULL,
    modified_date TIMESTAMP NOT NULL,
    metadata_json JSON,
    
    -- Processing tracking
    _last_processed_at TIMESTAMP NOT NULL
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_str_accounts_customer ON structured.str_accounts(customer_id);
CREATE INDEX IF NOT EXISTS idx_str_accounts_type ON structured.str_accounts(account_type);
CREATE INDEX IF NOT EXISTS idx_str_accounts_processed ON structured.str_accounts(_last_processed_at);


