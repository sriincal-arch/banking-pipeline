/*
    Test: Verify all accounts have a matching customer
    
    This test ensures referential integrity between accounts and customers
    at the structured layer level.
    
    Expected: 0 rows (all accounts should have a valid customer reference)
*/

select
    a.account_id,
    a.customer_id
from {{ ref('str_accounts') }} a
left join {{ ref('str_customers') }} c on a.customer_id = c.customer_id
where c.customer_id is null

