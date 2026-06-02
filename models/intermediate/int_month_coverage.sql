select 
    date_trunc(date_of_transfer, month) as year_month,
    count(*) as sales_count
from {{ ref('stg_ppd_all_transactions') }}
group by 1
order by 1