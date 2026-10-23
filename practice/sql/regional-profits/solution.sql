from pyspark.sql import functions as F



result_df = (orders
              .filter(F.col("region").isNotNull())
              .groupBy("region")
              .agg(F.sum("profit").alias("total_profit"),
                    F.count("order_id").alias("order_count"))
              .orderBy(F.col("total_profit").desc())
            )


# select region,  sum(profit) as total_profit, count(order_id) as order_count
#  from orders
#  where region is not null
#  group by region
#  order by sum(profit) desc
