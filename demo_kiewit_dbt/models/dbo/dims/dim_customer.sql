{{
  config(
    materialized = 'table',
    transient = false,
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'dbo',
    tags         = ['dbo', 'gold']
  )
}}


with customer as (
    select * from {{ ref('stg_customer') }}
)


select
    customer_id,
    full_name,
    first_name,
    last_name,
    name_initials,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _updated_at
from customer
