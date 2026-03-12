{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'dbo',
    tags         = ['dbo', 'gold']
  )
}}

with src as (
    select * from {{ ref('stg_discount') }}
),

dedup as (
    select
        discount_id,
        customer_id,
        discount_pct,
        valid_from,
        valid_to,
        is_active,
        row_number() over (partition by discount_id order by valid_from desc) as rn
    from src
)

select
    discount_id        as discount_key,
    customer_id,
    discount_pct,
    valid_from,
    valid_to,
    is_active,
    current_timestamp() as _updated_at
from dedup
where rn = 1