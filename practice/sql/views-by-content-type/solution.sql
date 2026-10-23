from pyspark import functions as F


# views = content_views
#             .groupby("content_id")
#              .agg(F.count("view_id").alias("view_count"))


# result = content_items
#           .join(views,"content_id","inner")
#           .groupBy("content_type")
#           .agg(F.sum("view_count").alias("view_count"))
          


from pyspark import functions as F

result = (
  content_items
  .join(content_views, "content_id", "inner")
  .groupBy("content_type")
  .agg(F.count("view_id").alias("view_count"))
)
