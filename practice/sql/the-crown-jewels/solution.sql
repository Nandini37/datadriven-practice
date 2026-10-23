select top 5 product_name, sum(total_amount) from products p 
join transactions t 
on p.product_id = t.product_id 
group by product_name
order by sum(total_amount) desc
