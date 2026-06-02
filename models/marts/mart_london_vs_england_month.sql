with base as (
  select
    date_trunc(date_of_transfer, month) as year_month,
    county,
    price
  from {{ ref('stg_ppd_all_transactions') }}
  where date_of_transfer is not null
    and price is not null
),

england as (
  select
    year_month,
    count(*) as sales_count_england,
    avg(price) as avg_price_england,
    approx_quantiles(price, 2)[offset(1)] as median_price_england
  from base
  group by 1
),

london as (
  select
    year_month,
    count(*) as sales_count_london,
    avg(price) as avg_price_london,
    approx_quantiles(price, 2)[offset(1)] as median_price_london
  from base
  where county = 'GREATER LONDON'
  group by 1
),

cpih as (
  select
    year_month,
    cpih_index
  from {{ ref('stg_ons_cpih_index_monthly') }}
),

base_cpih as (
  select cpih_index as base_cpih_index
  from cpih
  where year_month = date '2012-01-01'
)

select
  e.year_month,
  e.sales_count_england,
  l.sales_count_london,
  e.avg_price_england,
  l.avg_price_london,
  e.median_price_england,
  l.median_price_london,
  (l.avg_price_london - e.avg_price_england) as avg_price_premium,
  safe_divide(l.avg_price_london, e.avg_price_england) as avg_price_ratio,
  c.cpih_index,
  b.base_cpih_index,

  -- “Real” prices rebased to base_cpih_index
  (e.avg_price_england * (b.base_cpih_index / c.cpih_index)) as real_avg_price_england,
  (l.avg_price_london  * (b.base_cpih_index / c.cpih_index)) as real_avg_price_london,
  (e.median_price_england * (b.base_cpih_index / c.cpih_index)) as real_median_price_england,
  (l.median_price_london  * (b.base_cpih_index / c.cpih_index)) as real_median_price_london

from england e
left join london l using (year_month)
left join cpih c using (year_month)
cross join base_cpih b
order by e.year_month
