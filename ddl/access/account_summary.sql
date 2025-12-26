-- Access Layer: Account Summary Table
-- Final output table for business reporting and CSV export

CREATE TABLE IF NOT EXISTS access.account_summary (
    -- Output columns matching business requirements
    customer_id VARCHAR NOT NULL,
    account_id VARCHAR PRIMARY KEY,
    original_balance DECIMAL(15, 2) NOT NULL,
    interest_rate DECIMAL(5, 4) NOT NULL,
    annual_interest DECIMAL(15, 2) NOT NULL,
    new_balance DECIMAL(15, 2) NOT NULL
);

-- Index for query performance
CREATE INDEX IF NOT EXISTS idx_account_summary_customer ON access.account_summary(customer_id);


