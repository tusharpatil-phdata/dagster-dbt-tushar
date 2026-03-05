{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'lz',
    tags         = ['lz', 'bronze']
  )
}}

select
    id                             as store_id,
    name                           as store_name,
    opened_at::timestamp_ntz       as opened_at_utc,
    to_date(opened_at)             as opened_date,
    tax_rate::number(5,4)          as tax_rate,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _loaded_at,
    'store.csv'                    as _source_file
from {{ source('raw_store', 'STORE') }}