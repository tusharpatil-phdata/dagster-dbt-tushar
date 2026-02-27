{{
  config(
    materialized     = 'incremental',
    database         = 'CUSTOMERS_DB',
    schema           = 'lz',
    unique_key       = 'customer_id',
    on_schema_change = 'sync_all_columns',
    tags             = ['lz', 'bronze']
  )
}}

with source as (
    select * from {{ ref('customer') }}
)

select
    id                  as customer_id,
    name                as full_name,
    current_timestamp() as _loaded_at,
    'customer.csv'      as _source_file
from source

{% if is_incremental() %}
    where id not in (select customer_id from {{ this }})
{% endif %}