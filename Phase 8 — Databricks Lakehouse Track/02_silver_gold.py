import dlt
from pyspark.sql.functions import col, expr, when

@dlt.table(
    name="silver_clickstream",
    comment="Cleansed clickstream data matching the raw event categories.",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_event", "event_type IN ('add_to_cart', 'checkout_start', 'click_ad', 'search', 'view_item', 'remove_from_cart')")
@dlt.expect_or_drop("valid_timestamp", "event_timestamp IS NOT NULL AND event_timestamp != ''")
def silver_clickstream():
    return (
        dlt.readStream("main.clickstream_schema.bronze_clickstream")
        .dropDuplicates(["session_id"])
        )
    )