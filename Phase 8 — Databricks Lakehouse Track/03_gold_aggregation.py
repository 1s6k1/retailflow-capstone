from pyspark.sql.functions import col, count

silver_df = spark.read.table("main.clickstream_schema.silver_clickstream")

# Aggregate using actual schema columns
gold_df = (silver_df
           .groupBy("event_type")
           .agg(
               count("session_id").alias("total_events"),
               count(col("customer_id")).alias("unique_customers")
           ))


gold_df.write.format("delta").mode("overwrite").saveAsTable("main.clickstream_schema.gold_metrics")