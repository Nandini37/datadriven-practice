select creator_id, avg(duration_seconds) as avg_durations  from content_items
where duration_seconds is not null 
group by creator_id
having sum(duration_seconds) > 3600
order by avg(duration_seconds) desc , publish_date asc
