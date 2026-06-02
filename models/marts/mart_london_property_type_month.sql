with base as (
    select 
        date_trunc(date_of_transfer, month) as year_month,
        property_type, 
        price
    from {{ ref('stg_ppd_all_transactions') }}
    where date_of_transfer is not null
      and price is not null
      and property_type is not null
      and county = 'GREATER LONDON'
),

agg as (
    select 
        year_month,
        property_type,
        count(*) as sales_count,
        avg(price) as avg_price,
        approx_quantiles(price, 2)[OFFSET(1)] as median_price
    from base
    group by 1, 2
),

with_lags as (
    select
        *,
        lag(median_price, 12) over (partition by property_type order by year_month) as median_price_12m_ago
    from agg
)

select 
    year_month,
    property_type,
    sales_count,
    avg_price,
    median_price,
    median_price_12m_ago,
    safe_divide(median_price - median_price_12m_ago, median_price_12m_ago) as yoy_median_price_change
from with_lags
order by year_month, property_type