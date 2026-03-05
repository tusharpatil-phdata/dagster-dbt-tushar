{{
  config(
    materialized = 'view',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'staging',
    tags         = ['staging', 'silver']
  )
}}

with source as (
    select * from {{ ref('raw_supply') }}
)

select
    supply_id,
    supply_name,
    cost_cents/100.0 as supply_cost,
    is_perishable,
    sku
from source