{{
  config(
    materialized = 'incremental',
    unique_key = 'transaction_id',
    on_schema_change = 'sync_all_columns',
    partition_by = {'field': 'date_of_transfer', 'data_type': 'date', 'granularity': 'day'}

  )
}}

with src as (
  select * from {{ source('property_raw', 'ppd_all_transactions') }}

  {% if is_incremental() %}
    where cast(substr(date_of_transfer_raw, 1, 10) as date) > (select max(date_of_transfer) from {{ this }})
  {% endif %}
),

clean as (
  select
    transaction_id,
    cast(price as int64) as price,
    -- your file is like '2019-05-24 00:00'
    cast(substr(date_of_transfer_raw, 1, 10) as date) as date_of_transfer,
    upper(trim(postcode)) as postcode,
    upper(trim(property_type)) as property_type,
    upper(trim(old_new)) as old_new,
    upper(trim(duration)) as duration,
    paon, saon, street, locality,
    upper(trim(town_city)) as town_city,
    upper(trim(district)) as district,
    upper(trim(county)) as county,
    case when county = 'GREATER LONDON' then true else false end as is_london,
    upper(trim(ppd_category_type)) as ppd_category_type,
    upper(trim(record_status)) as record_status
  from src
  where cast(substr(date_of_transfer_raw, 1, 10) as date) >= '2015-01-01'
    and cast(price as int64) > 1000
)

select * from clean
