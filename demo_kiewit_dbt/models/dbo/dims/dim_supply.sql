{{
  config(
    materialized = 'table',
    transient = false,
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'dbo',
    tags         = ['dbo', 'gold']
  )
}}

with supply as (
    select
        s.supply_id,
        s.supply_name,
        s.supply_cost,
        s.is_perishable,
        s.sku,
        p.product_key,
        p.product_name,
        p.product_type
    from {{ ref('stg_supply') }} s
    left join {{ ref('dim_product') }} p
      on s.sku = p.product_key
)

select
    supply_id          as supply_key,
    supply_name,
    supply_cost,
    is_perishable,
    sku,
    product_key,
    product_name,
    product_type,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _updated_at
from supply