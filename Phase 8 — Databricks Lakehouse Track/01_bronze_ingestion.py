
# Define paths using your S3 External Location
source_path = "s3://retailflow-bucket-2026/raw/clickstream/"
checkpoint_path = "s3://retailflow-bucket-2026/checkpoints/bronze/"
schema_path = "s3://retailflow-bucket-2026/schemas/bronze/"
target_table = "main.clickstream_schema.bronze_clickstream"

# Read streaming data using Auto Loader
df_bronze = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.useNotifications", "false") # Forces Directory-listing mode
             .option("cloudFiles.schemaLocation", schema_path) # FIXED: Pointing safely to S3
             .load(source_path))

# Write to Delta table with checkpointing
query = (df_bronze.writeStream
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", checkpoint_path)
         .trigger(availableNow=True)
         .toTable(target_table))

query.awaitTermination()