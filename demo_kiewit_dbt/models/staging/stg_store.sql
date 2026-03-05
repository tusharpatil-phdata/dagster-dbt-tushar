{{
  config(
    materialized = 'view',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'staging',
    tags         = ['staging', 'silver']
  )
}}

with source as (
    select * from {{ ref('raw_store') }}
)

select
    store_id,
    store_name,
    opened_at_utc,
    opened_date,
    tax_rate,
    datediff('day', opened_date, current_date) as store_age_days
from source