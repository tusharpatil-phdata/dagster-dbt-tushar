{{
  config(
    materialized = 'view',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'staging',
    tags         = ['staging', 'silver']
  )
}}

with source as (
    select * from {{ ref('raw_discount') }}
),

cleaned as (
    select
        discount_id,
        customer_id,
        cast(discount_percent_raw as number(5,2))           as discount_pct,
        to_timestamp_ntz(valid_from_raw)                    as valid_from,
        to_timestamp_ntz(valid_to_raw)                      as valid_to,
        case
            when current_timestamp() between to_timestamp_ntz(valid_from_raw)
                                        and to_timestamp_ntz(valid_to_raw)
            then true else false
        end                                                as is_active
    from source
)

select * from cleaned