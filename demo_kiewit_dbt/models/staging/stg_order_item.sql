{{
  config(
    materialized = 'view',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'staging',
    tags         = ['staging', 'silver']
  )
}}

with source as (
    select * from {{ ref('raw_order_item') }}
),

joined as (
    select
        oi.order_item_id,
        oi.order_id,
        oi.sku,
        p.name as product_name,
        p.type as product_type
    from source oi
    left join {{ ref('raw_product') }} p
      on oi.sku = p.sku
)

select * from joined