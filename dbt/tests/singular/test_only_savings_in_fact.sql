/*
    Test: Verify that only savings accounts appear in fact_customer_balances
    
    This test ensures the business rule that only savings accounts are eligible
    for interest calculations is properly enforced.
    
    Expected: 0 rows (no non-savings accounts should exist in the fact table)
*/

select
    f.account_id,
    f.account_type
from {{ ref('fact_customer_balances') }} f
where lower(f.account_type) != 'savings'

