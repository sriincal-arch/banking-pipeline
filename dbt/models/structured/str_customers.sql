{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge',
        schema='structured'
    )
}}

/*
    Structured Layer: str_customers
    
    Processing Logic:
    1. Pick up raw records after last processed timestamp
    2. Apply data cleansing and standardization
    3. Deduplicate by customer_id, keeping the latest record by file arrival timestamp
    4. Merge into target (upsert)
*/

with source as (
    select
        *,
        -- Cast _ingested_at to timestamp for proper comparison
        cast(_ingested_at as timestamp) as ingested_ts
    from {{ source('raw', 'raw_customers') }}
    {% if is_incremental() %}
    where cast(_ingested_at as timestamp) > (
        select coalesce(max(_last_processed_at), timestamp '1900-01-01') 
        from {{ this }}
    )
    {% endif %}
),

cleaned as (
    select
        -- Clean and standardize customer_id
        trim(cast(CustomerID as varchar)) as customer_id,
        
        -- Clean name: trim whitespace and apply proper casing (title case)
        -- Use simple approach: uppercase first letter, lowercase rest for each word
        (
            select string_agg(
                upper(substring(word, 1, 1)) || lower(substring(word, 2)),
                ' '
            )
            from unnest(string_split(trim(cast(Name as varchar)), ' ')) as t(word)
            where word != ''
        ) as customer_name,
        
        -- Standardize boolean hasloan field
        {{ standardize_boolean('HasLoan') }} as hasloan,
        
        -- Metadata for audit trail
        _source_file,
        _file_hash,
        _row_number,
        ingested_ts as _ingested_at
        
    from source
    where 
        -- Filter out records with null primary key
        CustomerID is not null 
        and trim(cast(CustomerID as varchar)) != ''
),

-- Deduplicate by customer_id, keeping the latest by file arrival timestamp
deduplicated as (
    select *
    from (
        select 
            *,
            row_number() over (
                partition by customer_id 
                order by _ingested_at desc, _row_number desc
            ) as rn
        from cleaned
    )
    where rn = 1
),

final as (
    select
        customer_id,
        customer_name,
        hasloan,
        
        -- Audit columns
        {% if is_incremental() %}
        coalesce(
            (select cast(created_date as timestamp) from {{ this }} t where t.customer_id = deduplicated.customer_id),
            cast(current_timestamp as timestamp)
        ) as created_date,
        {% else %}
        cast(current_timestamp as timestamp) as created_date,
        {% endif %}
        cast(current_timestamp as timestamp) as modified_date,
        
        -- Metadata JSON with source lineage
        json_object(
            'source_file', _source_file,
            'row_number', cast(_row_number as varchar),
            'file_hash', _file_hash,
            'ingested_at', cast(_ingested_at as varchar)
        ) as metadata_json,
        
        -- Processing timestamp for incremental tracking (as timestamp)
        _ingested_at as _last_processed_at
        
    from deduplicated
)

select * from final

