{{
    config(
        materialized='incremental',
        unique_key='account_id',
        incremental_strategy='merge',
        schema='curated'
    )
}}

/*
    Curated Layer: fact_customer_balances
    
    Fact table containing interest calculations for savings accounts.
    Incremental processing picks up accounts/customers modified after last run.
    Only savings accounts are eligible for interest calculations.
*/

with accounts as (
    select * from {{ ref('str_accounts') }}
    where account_type = 'savings'  -- Only savings accounts are eligible for interest
    {% if is_incremental() %}
    and cast(modified_date as timestamp) > (
        select coalesce(max(cast(modified_date as timestamp)), timestamp '1900-01-01') 
        from {{ this }}
    )
    {% endif %}
),

customers as (
    select * from {{ ref('str_customers') }}
),

joined as (
    select
        a.account_id,
        a.customer_id,
        c.customer_name,
        a.account_type,
        a.original_balance,
        c.hasloan,
        a.created_date as account_created_date,
        a.modified_date as account_modified_date,
        a.metadata_json as account_metadata
    from accounts a
    inner join customers c on a.customer_id = c.customer_id
),

with_interest as (
    select
        account_id,
        customer_id,
        customer_name,
        account_type,
        original_balance,
        hasloan,
        
        -- Calculate interest rate based on balance tiers and loan bonus
        {{ calculate_interest_rate('original_balance', 'hasloan') }} as interest_rate,
        
        account_created_date,
        account_modified_date,
        account_metadata
    from joined
),

final as (
    select
        account_id,
        customer_id,
        customer_name,
        account_type,
        original_balance,
        hasloan,
        interest_rate,
        
        -- Calculate annual interest
        {{ calculate_annual_interest('original_balance', 'interest_rate') }} as annual_interest,
        
        -- Calculate new balance
        {{ calculate_new_balance('original_balance', calculate_annual_interest('original_balance', 'interest_rate')) }} as new_balance,
        
        -- Audit columns
        {% if is_incremental() %}
        coalesce(
            (select cast(created_date as timestamp) from {{ this }} t where t.account_id = with_interest.account_id),
            cast(current_timestamp as timestamp)
        ) as created_date,
        {% else %}
        cast(current_timestamp as timestamp) as created_date,
        {% endif %}
        cast(current_timestamp as timestamp) as modified_date,
        account_metadata as metadata_json
    from with_interest
)

select * from final

