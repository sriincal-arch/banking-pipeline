{{
    config(
        materialized='incremental',
        unique_key='account_id',
        incremental_strategy='merge',
        schema='curated'
    )
}}

/*
    Curated Layer: dim_account
    
    SCD Type 1 dimension - latest state of account attributes.
    Ensures ALL account data is persisted in the curated layer.
    Incremental processing uses merge strategy to update existing records
    and insert new ones, ensuring complete account coverage.
*/

with all_accounts as (
    select * from {{ ref('str_accounts') }}
),

existing_accounts as (
    {% if is_incremental() %}
    select account_id, modified_date as existing_modified_date
    from {{ this }}
    {% else %}
    -- Empty result set for full refresh
    select 
        cast(null as varchar) as account_id,
        cast(null as timestamp) as existing_modified_date
    where 1=0
    {% endif %}
),

accounts_to_process as (
    select 
        a.*
    from all_accounts a
    {% if is_incremental() %}
    left join existing_accounts e on a.account_id = e.account_id
    where 
        -- Process new accounts (not in existing table)
        e.account_id is null
        -- Or accounts that have been modified since last run
        or a.modified_date > coalesce(e.existing_modified_date, '1900-01-01'::timestamp)
    {% endif %}
)

select
    account_id,
    account_type,
    original_balance,
    customer_id,
    created_date,
    modified_date,
    metadata_json
from accounts_to_process

