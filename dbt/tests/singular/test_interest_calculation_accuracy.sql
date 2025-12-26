/*
    Test: Verify interest calculations are mathematically correct
    
    This test ensures:
    1. annual_interest = original_balance * interest_rate
    2. new_balance = original_balance + annual_interest
    
    Expected: 0 rows (all calculations should match)
*/

select
    account_id,
    original_balance,
    interest_rate,
    annual_interest,
    new_balance,
    round(original_balance * interest_rate, 2) as expected_annual_interest,
    round(original_balance + round(original_balance * interest_rate, 2), 2) as expected_new_balance
from {{ ref('fact_customer_balances') }}
where 
    round(annual_interest, 2) != round(original_balance * interest_rate, 2)
    or round(new_balance, 2) != round(original_balance + annual_interest, 2)


