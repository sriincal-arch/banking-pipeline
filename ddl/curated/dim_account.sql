-- Curated Layer: Account Dimension Table
-- SCD Type 1 - Latest state of account attributes

CREATE TABLE IF NOT EXISTS curated.dim_account (
    -- Surrogate and natural keys
    account_id VARCHAR PRIMARY KEY,
    
    -- Dimension attributes
    account_type VARCHAR NOT NULL,
    original_balance DECIMAL(15, 2) NOT NULL,
    customer_id VARCHAR NOT NULL,
    
    -- Audit columns
    created_date TIMESTAMP NOT NULL,
    modified_date TIMESTAMP NOT NULL,
    metadata_json JSON,
    
    -- Foreign key reference
    FOREIGN KEY (customer_id) REFERENCES curated.dim_customer(customer_id)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_dim_account_customer ON curated.dim_account(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_account_type ON curated.dim_account(account_type);


