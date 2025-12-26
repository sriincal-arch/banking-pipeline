-- Curated Layer: Fact Customer Balances Table
-- Contains interest calculations for savings accounts

CREATE TABLE IF NOT EXISTS curated.fact_customer_balances (
    -- Primary key
    account_id VARCHAR PRIMARY KEY,
    
    -- Foreign keys
    customer_id VARCHAR NOT NULL,
    
    -- Dimension attributes (denormalized for performance)
    customer_name VARCHAR NOT NULL,
    account_type VARCHAR NOT NULL,
    hasloan BOOLEAN NOT NULL,
    
    -- Measures
    original_balance DECIMAL(15, 2) NOT NULL,
    interest_rate DECIMAL(5, 4) NOT NULL,
    annual_interest DECIMAL(15, 2) NOT NULL,
    new_balance DECIMAL(15, 2) NOT NULL,
    
    -- Audit columns
    created_date TIMESTAMP NOT NULL,
    modified_date TIMESTAMP NOT NULL,
    metadata_json JSON,
    
    -- Foreign key references
    FOREIGN KEY (customer_id) REFERENCES curated.dim_customer(customer_id),
    FOREIGN KEY (account_id) REFERENCES curated.dim_account(account_id)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_fact_balances_customer ON curated.fact_customer_balances(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_balances_rate ON curated.fact_customer_balances(interest_rate);

