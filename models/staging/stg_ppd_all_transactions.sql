{{
  config(
    materialized = 'table'
  )
}}

with src as (

    select *
    from {{ source('property_raw', 'ppd_all_transactions') }}
),

clean as (

    select

        transaction_id,

        safe_cast(price as int64) as price,
        cast(substr(date_of_transfer_raw, 1, 10) as date) as date_of_transfer,

        upper(trim(postcode)) as postcode,
        regexp_extract(upper(trim(postcode)), r'^[A-Z]+') as postcode_area,
        regexp_extract(upper(trim(postcode)), r'^[A-Z]{1,2}[0-9][0-9A-Z]?') as postcode_district,

        case
            when upper(trim(property_type)) = 'D' then 'DETACHED'
            when upper(trim(property_type)) = 'S' then 'SEMI_DETACHED'
            when upper(trim(property_type)) = 'T' then 'TERRACED'
            when upper(trim(property_type)) = 'F' then 'FLAT'
            when upper(trim(property_type)) = 'O' then 'OTHER'
            else 'UNKNOWN'
        end as property_type,

        case
            when upper(trim(old_new)) = 'Y' then 'NEW_BUILD'
            when upper(trim(old_new)) = 'N' then 'EXISTING'
            else 'UNKNOWN'
        end as build_status,

        case
            when upper(trim(duration)) = 'F' then 'FREEHOLD'
            when upper(trim(duration)) = 'L' then 'LEASEHOLD'
            else 'UNKNOWN'
        end as tenure,

        upper(trim(paon)) as building_number,
        upper(trim(saon)) as unit_number,

        upper(trim(street)) as street,
        upper(trim(locality)) as locality,
        upper(trim(town_city)) as town_city,
        upper(trim(district)) as district,
        upper(trim(county)) as county,

        upper(trim(county)) = 'GREATER LONDON' as is_london,

        case
            when upper(trim(ppd_category_type)) = 'A' then 'STANDARD_PRICE_PAID'
            when upper(trim(ppd_category_type)) = 'B' then 'ADDITIONAL_PRICE_PAID'
            else 'UNKNOWN'
        end as transaction_category,

        case
            when upper(trim(record_status)) = 'A' then 'ACTIVE'
            when upper(trim(record_status)) = 'D' then 'DELETED'
            else 'UNKNOWN'
        end as record_status

    from src

    where cast(substr(date_of_transfer_raw, 1, 10) as date) >= date '2015-01-01'
      and safe_cast(price as int64) >= 1000

)

select *
from clean