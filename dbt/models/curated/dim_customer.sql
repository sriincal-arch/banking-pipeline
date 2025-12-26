{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge',
        schema='curated'
    )
}}

/*
    Curated Layer: dim_customer
    
    SCD Type 1 dimension - latest state of customer attributes.
    Incremental processing picks up records modified after last run.
*/

with customers as (
    select * from {{ ref('str_customers') }}
    {% if is_incremental() %}
    where modified_date > (
        select coalesce(max(modified_date), '1900-01-01'::timestamp) 
        from {{ this }}
    )
    {% endif %}
)

select
    customer_id,
    customer_name,
    hasloan,
    created_date,
    modified_date,
    metadata_json
from customers

