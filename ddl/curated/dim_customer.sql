-- Curated Layer: Customer Dimension Table
-- SCD Type 1 - Latest state of customer attributes

CREATE TABLE IF NOT EXISTS curated.dim_customer (
    -- Surrogate and natural keys
    customer_id VARCHAR PRIMARY KEY,
    
    -- Dimension attributes
    customer_name VARCHAR NOT NULL,
    hasloan BOOLEAN NOT NULL,
    
    -- Audit columns
    created_date TIMESTAMP NOT NULL,
    modified_date TIMESTAMP NOT NULL,
    metadata_json JSON
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_dim_customer_name ON curated.dim_customer(customer_name);
CREATE INDEX IF NOT EXISTS idx_dim_customer_loan ON curated.dim_customer(hasloan);


