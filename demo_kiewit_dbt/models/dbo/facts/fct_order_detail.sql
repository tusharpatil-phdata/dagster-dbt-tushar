{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'dbo',
    tags         = ['dbo', 'gold']
  )
}}

with orders as (
    select * from {{ ref('stg_order_detail') }}
),

joined as (
    select
        o.order_id,
        o.order_date,
        o.ordered_at_utc,
        o.customer_id,
        o.store_id,
        c.full_name as customer_name,
        s.store_name,
        o.subtotal,
        o.tax_paid,
        o.order_total,
        o.has_revenue
    from orders o
    left join {{ ref('dim_customer') }} c
      on o.customer_id = c.customer_id
    left join {{ ref('dim_store') }} s
      on o.store_id = s.store_key
)

select
    *,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _updated_at
from joined