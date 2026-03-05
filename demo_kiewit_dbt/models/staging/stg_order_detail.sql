{{
  config(
    materialized = 'view',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'staging',
    tags         = ['staging', 'silver']
  )
}}

with source as (
    select * from {{ ref('raw_order_detail') }}
)

select
    order_id,
    customer_id,
    store_id,
    ordered_at_utc,
    order_date,
    subtotal,
    tax_paid,
    order_total,
    case when order_total > 0 then 1 else 0 end as has_revenue
from source