{{
  config(
    materialized = 'view',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'staging',
    tags         = ['staging', 'silver']
  )
}}

with source as (
    select * from {{ ref('raw_product') }}
)

select
    sku               as product_key,
    name              as product_name,
    type              as product_type,
    price_cents/100.0 as price,
    description
from source