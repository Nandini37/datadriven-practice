from pyspark import functions as F

result_df = (
          page_views      
            .filter(
              (F.to_date(F.col("viewed_at")) >= "2025-01-01") &
              (F.to_date(F.col("viewed_at")) <= "2025-12-28")
              )
            .groupBy("user_id")
            .agg(F.count("view_id").alias("total_views"))

            )


# Alternate solution


# from pyspark.sql import functions as F

# result_df = (
#   page_views
#   .filter("to_date(viewed_at) BETWEEN '2025-01-01' AND '2025-12-28'")
#   .groupBy("user_id")
#   .agg(F.count("view_id").alias("total_views"))
# )
