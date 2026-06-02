select
  cast(month as date) as year_month,
  cast(cpih_index as float64) as cpih_index
from {{ source('property_raw', 'ons_cpih_index_monthly') }}
