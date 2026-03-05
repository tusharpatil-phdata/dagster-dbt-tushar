{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'lz',
    tags         = ['lz', 'bronze']
  )
}}

select
    id                             as order_id,
    customer                       as customer_id,
    ordered_at::timestamp_ntz      as ordered_at_utc,
    to_date(ordered_at)            as order_date,
    store_id                       as store_id,
    subtotal::number(10,2)         as subtotal,
    tax_paid::number(10,2)         as tax_paid,
    order_total::number(10,2)      as order_total,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _loaded_at,
    'order_detail.csv'             as _source_file
from {{ source('raw_order_detail', 'ORDER_DETAIL') }}