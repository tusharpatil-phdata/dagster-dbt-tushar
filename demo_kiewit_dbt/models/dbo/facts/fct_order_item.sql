/*
{{
  config(
    materialized = 'table',
    transient = false,
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'dbo',
    tags         = ['dbo', 'gold']
  )
}}

with line_items as (
    select * from {{ ref('stg_order_item') }}
)

select
    order_item_id,
    order_id,
    sku,
    product_name,
    product_type,
    1 as quantity,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _updated_at
from line_items
*/

{{
  config(
    materialized = 'incremental',
    unique_key = 'order_item_id',
    transient = false,
    database = 'DAGSTER_DBT_KIEWIT_DB',
    schema = 'dbo',
    tags = ['dbo', 'gold']
  )
}}

with line_items as (
    select * from {{ ref('stg_order_item') }}
)
select
    order_item_id,
    order_id,
    sku,
    product_name,
    product_type,
    1 as quantity,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _updated_at
from line_items
{% if is_incremental() %}
    where order_item_id not in (select order_item_id from {{ this }})
{% endif %}