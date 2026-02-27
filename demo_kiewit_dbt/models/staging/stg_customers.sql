{{
  config(
    materialized = 'table',
    database     = 'CUSTOMERS_DB',
    schema       = 'staging',
    tags         = ['staging', 'silver']
  )
}}

with source as (
    select * from {{ ref('lz_customers') }}
),

cleaned as (
    select
        customer_id,
        initcap(trim(full_name))                                as full_name,
        initcap(trim(split_part(trim(full_name), ' ', 1)))      as first_name,
        nullif(
            initcap(trim(
                case
                    when position(' ' in trim(full_name)) > 0
                    then substr(
                            trim(full_name),
                            position(' ' in trim(full_name)) + 1
                         )
                    else ''
                end
            )),
            ''
        )                                                       as last_name,
        upper(left(split_part(trim(full_name), ' ', 1), 1))
            || coalesce(
                upper(left(
                    nullif(
                        substr(
                            trim(full_name),
                            position(' ' in trim(full_name)) + 1
                        ), ''
                    ), 1
                )),
                ''
            )                                                   as name_initials,
        _loaded_at,
        _source_file
    from source
    where customer_id is not null
      and full_name    is not null
),

deduped as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by _loaded_at desc
        ) as _rn
    from cleaned
)

select * exclude (_rn)
from deduped
where _rn = 1