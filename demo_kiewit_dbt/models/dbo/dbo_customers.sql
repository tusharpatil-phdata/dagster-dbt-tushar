{{
  config(
    materialized     = 'incremental',
    database         = 'CUSTOMERS_DB',
    schema           = 'dbo',
    unique_key       = 'customer_id',
    transient        = false,
    on_schema_change = 'sync_all_columns',
    tags             = ['dbo', 'gold']
  )
}}

with base as (
    select * from {{ ref('stg_customers') }}
)

select
    customer_id,
    full_name,
    first_name,
    last_name,
    name_initials,
    _loaded_at,
    current_timestamp() as _updated_at
from base

{% if is_incremental() %}
    where customer_id not in (select customer_id from {{ this }})
{% endif %}