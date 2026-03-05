{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'dbo',
    tags         = ['dbo', 'gold']
  )
}}

select
    product_key,
    product_name,
    product_type,
    price,
    description,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _updated_at
from {{ ref('stg_product') }}