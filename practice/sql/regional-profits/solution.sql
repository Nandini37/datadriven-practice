select region,  sum(profit) as total_profit, count(order_id) as order_count
 from orders
 where region is not null
 group by region
 order by sum(profit) desc


-- select * from orders
