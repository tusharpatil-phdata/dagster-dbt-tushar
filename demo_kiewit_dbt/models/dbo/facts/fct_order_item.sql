{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'dbo',
    tags         = ['dbo', 'gold']
  )
}}

with line_items as (
    select * from {{ ref('stg_order_item') }}
)

select
    order_item_id,
    order_id,
    sku,
    product_name,
    product_type,
    1 as quantity
from line_items