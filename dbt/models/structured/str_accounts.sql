{{
    config(
        materialized='incremental',
        unique_key='account_id',
        incremental_strategy='merge',
        schema='structured'
    )
}}

/*
    Structured Layer: str_accounts
    
    Processing Logic:
    1. Pick up raw records after last processed timestamp
    2. Apply data cleansing and standardization
    3. Deduplicate by account_id, keeping the latest record by file arrival timestamp
    4. Merge into target (upsert)
*/

with source as (
    select
        *,
        -- Cast _ingested_at to timestamp for proper comparison
        cast(_ingested_at as timestamp) as ingested_ts
    from {{ source('raw', 'raw_accounts') }}
    {% if is_incremental() %}
    where cast(_ingested_at as timestamp) > (
        select coalesce(max(_last_processed_at), timestamp '1900-01-01') 
        from {{ this }}
    )
    {% endif %}
),

cleaned as (
    select
        -- Clean and standardize account_id
        trim(cast(AccountID as varchar)) as account_id,

        -- Normalize account_type to lowercase
        lower(trim(cast(AccountType as varchar))) as account_type,

        -- Clean balance: remove commas, convert to decimal
        -- Handle missing values: NULL balance -> 0.00 with data_quality_flag
        try_cast(
            replace(trim(cast(Balance as varchar)), ',', '') as decimal(15, 2)
        ) as original_balance_raw,

        -- Clean and standardize customer_id
        trim(cast(CustomerID as varchar)) as customer_id,

        -- Metadata for audit trail
        _source_file,
        _file_hash,
        _row_number,
        ingested_ts as _ingested_at

    from source
    where
        -- Filter out records with null primary key
        AccountID is not null
        and trim(cast(AccountID as varchar)) != ''
),

-- Handle missing/malformed values with default and data quality flag
with_valid_balance as (
    select
        *,
        -- COALESCE NULL balance to 0.00 (requirement: handle missing values)
        coalesce(original_balance_raw, 0.00) as original_balance,

        -- Data quality flag: track which records had missing/imputed values
        case
            when original_balance_raw is null then 'IMPUTED_NULL_BALANCE'
            else 'VALID'
        end as data_quality_flag
    from cleaned
),

-- Deduplicate by account_id, keeping the latest by file arrival timestamp
deduplicated as (
    select *
    from (
        select 
            *,
            row_number() over (
                partition by account_id 
                order by _ingested_at desc, _row_number desc
            ) as rn
        from with_valid_balance
    )
    where rn = 1
),

final as (
    select
        account_id,
        account_type,
        original_balance,
        customer_id,
        data_quality_flag,  -- Track imputed/missing values

        -- Audit columns
        {% if is_incremental() %}
        coalesce(
            (select cast(created_date as timestamp) from {{ this }} t where t.account_id = deduplicated.account_id),
            cast(current_timestamp as timestamp)
        ) as created_date,
        {% else %}
        cast(current_timestamp as timestamp) as created_date,
        {% endif %}
        cast(current_timestamp as timestamp) as modified_date,

        -- Metadata JSON with source lineage and data quality info
        json_object(
            'source_file', _source_file,
            'row_number', cast(_row_number as varchar),
            'file_hash', _file_hash,
            'ingested_at', cast(_ingested_at as varchar),
            'data_quality_flag', data_quality_flag
        ) as metadata_json,

        -- Processing timestamp for incremental tracking (as timestamp)
        _ingested_at as _last_processed_at

    from deduplicated
)

select * from final

