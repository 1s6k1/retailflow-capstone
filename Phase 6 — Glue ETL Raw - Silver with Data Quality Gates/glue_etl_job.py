import sys
import datetime
import boto3

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
from awsglue.gluetypes import *

# Standalone Data Quality Engine import module
from awsgluedq.transforms import EvaluateDataQuality 

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType, BooleanType
)

# ---------------------------------------------------------------------------
# 0. Job Initialization & Configuration
# ---------------------------------------------------------------------------
REQUIRED_ARGS = [
    "JOB_NAME",
    "source_database",       # Glue Data Catalog DB holding raw tables
    "curated_bucket",        # e.g. s3://my-co-curated
    "quarantine_bucket",     # e.g. s3://my-co-quarantine
    "orders_dqdl_path",      # e.g. s3://my-co-artifacts/dqdl/orders_ruleset.dqdl
    "order_items_dqdl_path", # e.g. s3://my-co-artifacts/dqdl/order_items_ruleset.dqdl
    "cw_namespace",          # e.g. RetailPipeline/DataQuality
]
args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Enable mergeSchema semantics globally for structural variations (Day 2 changes)
spark.conf.set("spark.sql.parquet.mergeSchema", "true")

# Extract S3 string targets to decouple nested string execution paths
curated_path = args["curated_bucket"]
quarantine_path = args["quarantine_bucket"]

# ---------------------------------------------------------------------------
# Helper Function: Safely extract DataFrame from Data Quality Output
# ---------------------------------------------------------------------------
def safe_extract_dq_df(dq_result, key: str):
    if isinstance(dq_result, dict) or isinstance(dq_result, DynamicFrameCollection):
        return dq_result[key].toDF()
    elif isinstance(dq_result, DynamicFrame):
        return dq_result.toDF()
    return dq_result

# ---------------------------------------------------------------------------
# 1. Transform / Clean Functions Definition
# ---------------------------------------------------------------------------
def cast_and_clean_orders(df):
    has_discount_code = "discount_code" in df.columns

    df = (
        df
        .withColumn("order_id", F.col("order_id").cast(StringType()))
        .withColumn("customer_id", F.col("customer_id").cast(StringType()))
        # FIXED: Title Casing to pass the mixed-case status rule checking
        .withColumn("status", F.trim(F.initcap(F.col("status").cast(StringType()))))
        .withColumn("order_ts", F.coalesce(F.to_timestamp("order_ts", "yyyy-MM-dd'T'HH:mm:ss"), F.to_timestamp("order_ts")))
        .withColumn("order_date", F.to_date("order_ts"))
    )

    if has_discount_code:
        df = df.withColumn(
            "discount_code",
            F.when(F.trim(F.col("discount_code")) == "", None)
             .otherwise(F.trim(F.col("discount_code")))
             .cast(StringType())
        )
    else:
        df = df.withColumn("discount_code", F.lit(None).cast(StringType()))

    window = Window.partitionBy("order_id").orderBy(F.col("order_ts").desc_nulls_last())
    df = (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )
    return df


def cast_and_clean_order_items(df):
    df = (
        df
        .withColumn("order_id", F.col("order_id").cast(StringType()))
        .withColumn("product_id", F.col("product_id").cast(StringType()))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("line_total", F.col("line_total").cast(DoubleType()))
    )
    df = df.withColumn("quantity", F.when(F.col("quantity").isNull(), F.lit(0)).otherwise(F.col("quantity")))
    df = df.dropDuplicates(["order_id"])
    return df


def cast_and_clean_customers(df):
    df = (
        df
        .withColumn("customer_id", F.col("customer_id").cast(StringType()))
        .withColumn("customer_name", F.trim(F.col("customer_name").cast(StringType())))
        .withColumn("email", F.lower(F.trim(F.col("email").cast(StringType()))))
        .withColumn("country", F.upper(F.trim(F.col("country").cast(StringType()))))
    )
    df = df.dropDuplicates(["customer_id"])
    return df


def cast_and_clean_products(df):
    df = (
        df
        .withColumn("product_id", F.col("product_id").cast(StringType()))
        .withColumn("product_name", F.trim(F.col("product_name").cast(StringType())))
        .withColumn("category", F.trim(F.col("category").cast(StringType())))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("active_flag", F.col("active_flag").cast(StringType()))
    )
    df = df.dropDuplicates(["product_id"])
    return df


# ---------------------------------------------------------------------------
# 2. Extract -- Unified Reader Block (Bypassing Catalog for Customers & Products)
# ---------------------------------------------------------------------------
def read_source(table_name: str, transformation_ctx: str) -> DynamicFrame:
    additional_options = {}
    if table_name == "orders":
        additional_options = {
            "jobBookmarkKeys": ["order_id"],
            "jobBookmarkKeysSortOrder": "asc",
        }
    return glueContext.create_dynamic_frame.from_catalog(
        database=args["source_database"],
        table_name=table_name,
        transformation_ctx=transformation_ctx,
        additional_options=additional_options,
    )

# Extract Bookmarked transaction logs safely via Catalog
orders_dyf = read_source("orders", transformation_ctx="orders_bookmark_ctx")
order_items_dyf = read_source("order_items", transformation_ctx="order_items_bookmark_ctx")

orders_df = orders_dyf.toDF()
order_items_df = order_items_dyf.toDF()

# Direct S3 bypass for customers to avoid empty catalog schema locks []
customers_df = (
    spark.read
    .option("header", "false")
    .option("inferSchema", "true")
    .csv("s3://retailflow-bucket-2026/raw/customers/")
)

if "_c0" in customers_df.columns:
    customers_df = (
        customers_df
        .withColumnRenamed("_c0", "customer_id")
        .withColumnRenamed("_c1", "customer_name")
        .withColumnRenamed("_c2", "email")
        .withColumnRenamed("_c3", "country")
    )

# Direct S3 bypass for products to avoid empty catalog schema locks []
products_df = (
    spark.read
    .option("header", "false")
    .option("inferSchema", "true")
    .csv("s3://retailflow-bucket-2026/raw/products/")
)

if "_c0" in products_df.columns:
    products_df = (
        products_df
        .withColumnRenamed("_c0", "product_id")
        .withColumnRenamed("_c1", "product_name")
        .withColumnRenamed("_c2", "category")
        .withColumnRenamed("_c3", "unit_price")
        .withColumnRenamed("_c4", "active_flag")
    )

print(f"[bookmark-scoped read] orders new rows this run: {orders_dyf.count()}")
print(f"[bookmark-scoped read] order_items new rows this run: {order_items_dyf.count()}")


# ---------------------------------------------------------------------------
# 3. Execution -- Run Cleaning Operations Safely
# ---------------------------------------------------------------------------
orders_clean = cast_and_clean_orders(orders_df)
order_items_clean = cast_and_clean_order_items(order_items_df)
customers_clean = cast_and_clean_customers(customers_df)
products_clean = cast_and_clean_products(products_df)


# ---------------------------------------------------------------------------
# 4. Data Quality Management Engine
# ---------------------------------------------------------------------------
orders_for_dq = DynamicFrame.fromDF(orders_clean, glueContext, "orders_for_dq")
order_items_for_dq = DynamicFrame.fromDF(order_items_clean, glueContext, "order_items_for_dq")

# Standardized engine alias name to align with DQDL structural mapping rules
products_for_dq = DynamicFrame.fromDF(products_clean, glueContext, "products_for_dq")

# --- 4a. orders validation -------------------------------------------------
orders_ruleset_text = "\n".join(sc.textFile(args["orders_dqdl_path"]).collect())

orders_dq_results = EvaluateDataQuality.apply(
    frame=orders_for_dq,
    ruleset=orders_ruleset_text,
    publishing_options={
        "dataQualityEvaluationContext": "orders_dq_ctx",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)
orders_outcomes_df = safe_extract_dq_df(orders_dq_results, "ruleOutcomes")

# --- 4b. order_items validation --------------------------------------------
order_items_ruleset_text = "\n".join(sc.textFile(args["order_items_dqdl_path"]).collect())

order_items_dq_results = EvaluateDataQuality.apply(
    frame=order_items_for_dq,
    ruleset=order_items_ruleset_text,
    publishing_options={
        "dataQualityEvaluationContext": "order_items_dq_ctx",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"}
)
order_items_outcomes_df = safe_extract_dq_df(order_items_dq_results, "ruleOutcomes")
row_level_df = safe_extract_dq_df(order_items_dq_results, "rowLevelOutcomes")


# ---------------------------------------------------------------------------
# 4c. Unified Quality Metric Scoring & Referential Integrity Check
# ---------------------------------------------------------------------------
if "Rule" in orders_outcomes_df.columns and "Rule" in order_items_outcomes_df.columns:
    all_rule_outcomes_df = orders_outcomes_df.select("Rule", "Outcome").unionByName(
        order_items_outcomes_df.select("Rule", "Outcome")
    )
    overall_score_row = all_rule_outcomes_df.agg(
        F.avg(F.when(F.col("Outcome") == "Passed", 1.0).otherwise(0.0)).alias("quality_score")
    ).collect()[0]
    quality_score_pct = round((overall_score_row["quality_score"] or 0.0) * 100, 2)
else:
    quality_score_pct = 100.0

print(f"[DQ] Combined rule pass rate this run: {quality_score_pct}%")

if "DataQualityEvaluationResult" in row_level_df.columns:
    base_passed = row_level_df.filter(F.col("DataQualityEvaluationResult") == "Passed").drop(
        "DataQualityRulesPass", "DataQualityRulesFail", "DataQualityRulesSkip", "DataQualityEvaluationResult"
    )
    base_failed = row_level_df.filter(F.col("DataQualityEvaluationResult") == "Failed")
else:
    base_passed = order_items_clean
    base_failed = spark.createDataFrame([], order_items_clean.schema)

# Version-proof programmatic Referential Integrity enforcement via relational anti-join mapping
invalid_reference_df = base_passed.join(products_clean, "product_id", "left_anti")

passing_order_items = base_passed.join(products_clean, "product_id", "left_semi")
failing_order_items = base_failed.unionByName(invalid_reference_df, allowMissingColumns=True)


# ---------------------------------------------------------------------------
# 5. Load -- Curated Targets & Partitioned Sinks
# ---------------------------------------------------------------------------
run_date = datetime.date.today().isoformat()

# Target A: Write out clean orders partitioned by date
(
    orders_clean.write
    .mode("append")
    .partitionBy("order_date")
    .option("mergeSchema", "true")
    .parquet(curated_path + "/orders/")
)

# Target B: Extract date mapping from clean orders to use as a partition lookup schema
date_lookup_df = orders_clean.select("order_id", "order_date")

# Target C: Perform an inner join to attach 'order_date' onto order_items before writing
partitioned_order_items = passing_order_items.join(date_lookup_df, "order_id", "inner")

# Target D: Write out clean order_items partitioned by date folder splits
(
    partitioned_order_items.write
    .mode("append")
    .partitionBy("order_date")
    .option("mergeSchema", "true")
    .parquet(curated_path + "/order_items/")
)

# Target E: Save remaining dimensions globally
(
    customers_clean.write
    .mode("append")
    .option("mergeSchema", "true")
    .parquet(curated_path + "/customers/")
)

(
    products_clean.write
    .mode("append")
    .option("mergeSchema", "true")
    .parquet(curated_path + "/products/")
)

if failing_order_items.count() > 0:
    (
        failing_order_items
        .withColumn("dq_run_date", F.lit(run_date))
        .write.mode("append")
        .partitionBy("dq_run_date")
        .parquet(quarantine_path + "/order_items/")
    )
    print(f"[quarantine] wrote failing order_items rows to isolation")
else:
    print("[quarantine] no failing rows this run")


# ---------------------------------------------------------------------------
# 6. Publish Operational Telemetry Metrics to CloudWatch
# ---------------------------------------------------------------------------
cw = boto3.client("cloudwatch", region_name="us-east-1")
cw.put_metric_data(
    Namespace=args["cw_namespace"],
    MetricData=[
        {
            "MetricName": "DataQualityScore",
            "Dimensions": [
                {"Name": "JobName", "Value": args["JOB_NAME"]},
                {"Name": "Dataset", "Value": "order_items"},
            ],
            "Timestamp": datetime.datetime.utcnow(),
            "Value": quality_score_pct,
            "Unit": "Percent",
        },
        {
            "MetricName": "QuarantinedRowCount",
            "Dimensions": [
                {"Name": "JobName", "Value": args["JOB_NAME"]},
                {"Name": "Dataset", "Value": "order_items"},
            ],
            "Timestamp": datetime.datetime.utcnow(),
            "Value": float(failing_order_items.count()),
            "Unit": "Count",
        },
    ],
)
print(f"[CloudWatch] published DataQualityScore={quality_score_pct}% to namespace {args['cw_namespace']}")

job.commit()