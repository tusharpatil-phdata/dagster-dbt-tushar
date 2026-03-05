{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'lz',
    tags         = ['lz', 'bronze']
  )
}}

select
    id                             as order_item_id,
    order_id                       as order_id,
    sku                            as sku,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _loaded_at,
    'order_item.csv'               as _source_file
from {{ source('raw_order_item', 'ORDER_ITEM') }}