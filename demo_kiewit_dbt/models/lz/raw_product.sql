{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'lz',
    tags         = ['lz', 'bronze']
  )
}}

select
    sku                            as sku,
    name                           as name,
    type                           as type,
    price::number(10,2)            as price_cents,
    description                    as description,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _loaded_at,
    'product.csv'                  as _source_file
from {{ source('raw_product', 'PRODUCT') }}