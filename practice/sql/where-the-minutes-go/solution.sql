
with cte1 as (
select lower(device) ,sum(dur_ms) as total_dwell_ms 
from page_views
group by lower(device)
)

select *,
ROUND(
    100.0 * SUM(total_dwell_ms) OVER (ORDER BY total_dwell_ms DESC) 
    / SUM(total_dwell_ms) OVER (), 
    2
  ) AS running_pct
 from cte1
order by total_dwell_ms desc
