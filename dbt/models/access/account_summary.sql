{{
    config(
        materialized='table',
        schema='access'
    )
}}

/*
    Access Layer: Account Summary Report
    
    This model generates the final account summary report with interest calculations
    for all eligible savings accounts. Output is exported to account_summary.csv.
    
    Business Rules:
    - Only savings accounts are included in interest rate calculations
    - Explicitly filters for savings accounts to ensure data integrity
    - Interest rates are tiered based on balance:
        - < $10,000: 1%
        - $10,000 - $20,000: 1.5%
        - > $20,000: 2%
    - Customers with loans receive a 0.5% bonus
*/

with fact_data as (
    select * from {{ ref('fact_customer_balances') }}
    where lower(account_type) = 'savings'  -- Explicit filter: only savings accounts for interest calculation
)

select
    customer_id,
    account_id,
    original_balance,
    interest_rate,
    annual_interest,
    new_balance
from fact_data
order by customer_id, account_id

