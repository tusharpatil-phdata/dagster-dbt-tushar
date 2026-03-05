{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'lz',
    tags         = ['lz', 'bronze']
  )
}}

select
    id                             as supply_id,
    name                           as supply_name,
    cost::number(10,2)             as cost_cents,
    perishable::boolean            as is_perishable,
    sku                            as sku,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _loaded_at,
    'supply.csv'                   as _source_file
from {{ source('raw_supply', 'SUPPLY') }}