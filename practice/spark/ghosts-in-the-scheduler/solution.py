from pyspark import functions as F

df = (batch_jobs
        .filter(
          (F.col("ended").isNull()) &
          (F.lower(F.col("status")) == "completed") &
          (F.col("started").startswith("2024"))
          )
        .agg(F.count("job_id").alias("completed_without_end_count"))
        )
