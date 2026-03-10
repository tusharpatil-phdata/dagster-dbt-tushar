{{
  config(
    materialized = 'table',
    transient = false,
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'dbo',
    tags         = ['dbo', 'gold']
  )
}}

select
    store_id      as store_key,
    store_name,
    opened_date,
    tax_rate,
    store_age_days,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _updated_at
from {{ ref('stg_store') }}