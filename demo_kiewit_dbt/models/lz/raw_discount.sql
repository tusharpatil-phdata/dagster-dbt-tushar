{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'lz',
    tags         = ['lz', 'bronze']
  )
}}

select
    ID          as discount_id,
    CUSTOMER_ID as customer_id,
    PERCENT     as discount_percent_raw,
    VALID_FROM  as valid_from_raw,
    VALID_TO    as valid_to_raw,
    current_timestamp() as _loaded_at,
    'discounts.csv'     as _source_file
from {{ source('raw_discount', 'DISCOUNT') }}