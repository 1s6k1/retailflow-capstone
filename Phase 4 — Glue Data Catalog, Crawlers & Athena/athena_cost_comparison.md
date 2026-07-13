--------------------------------------------
ATHENA OPTIMIZATION & COST COMPARISON REPORT
--------------------------------------------

1) ENVIRONMENT & ARCHITECTURE OVERVIEW
    * Database Name: retailflow_raw
    * Tables Benchmarked: orders, order_items
    * Data Storage Location: s3://retailflow-bucket-2026/raw/
    * Partitioning Strategy: Explicit date partition folder format: dt=YYYY-MM-DD

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

2. CRAWLER EXECUTION & SCHEMA REMEDIATION

    THE ISSUE:
    When the AWS Glue Crawler scanned the raw S3 directory, it could not read proper headers from the source files. 
    As a result, it generated generic placeholder names for the columns:
    * Inferred Names: col1, col2, col3, col4

    THE FIX:
    The table schema was manually updated in the AWS Glue Console to map these placeholders to clean, readable fields:
    * col1 -> customer_id (STRING)
    * col2 -> customer_name (STRING)
    * col3 -> email (STRING)
    * col4 -> country (STRING)

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 
3. PARTITION PROJECTION CONFIGURATION
    * To avoid running a Glue Crawler every single day or typing "MSCK REPAIR TABLE" to find new folders, Partition Projection was turned on.

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

4. SQL command executed in Athena to set this up:

    ALTER TABLE retailflow_raw.orders SET TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.dt.type' = 'date',
    'projection.dt.range' = '2026-07-01,2026-07-31',
    'projection.dt.format' = 'yyyy-MM-dd',
    'storage.location.template' = 's3://retailflow-bucket-2026/raw/orders/dt=${dt}/'
    );

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

5. ATHENA QUERY BENCHMARKING & COST IMPACT
All tests were run in Amazon Athena. To prove the cost savings of partition pruning, each test was run twice: once scanning everything (Unpruned) and once using a WHERE clause on the "dt" partition (Pruned).

-----------------------------------------------------------
TEST 1: GLOBAL RETRIEVAL VS. TARGET SNAPSHOT (orders table)
-----------------------------------------------------------

Objective: Get order metrics globally vs. an isolated operational day.
1) Unpruned Query:
    select * from retailflow_raw.orders;
           
    * Data Scanned: 3.16 MB
    * Run Time: 1.431 seconds
    ![alt text](<1) orders_unpruned_query.png>)

2) Pruned Query:
    select * from retailflow_raw.orders where dt='2026-07-09';

    * Data Scanned: 618.23 KB
    * Run Time: 696 ms
    ![alt text](<1) orders_pruned_query.png>)

---------------------------------------------------------
TEST 2: STATUS VALUE GROUPING AGGREGATIONS (orders table)
---------------------------------------------------------

Objective: Count order statuses across history vs. a single day.
1) Unpruned Query:
    SELECT status, COUNT(*) as status_count FROM retailflow_raw.orders GROUP BY status;

    * Data Scanned: 3.16 MB
    * Run Time: 1.411 seconds
    ![alt text](<2) orders_unpruned_query.png>)

2) Pruned Query:
   SELECT status, COUNT(*) as status_count FROM retailflow_raw.orders where dt='2026-07-08' GROUP BY status;

    * Data Scanned: 2.55 MB
    * Run Time: 689 ms
    ![alt text](<2) orders_pruned_query.png>)

----------------------------------------------------------
TEST 3: REVENUE CONCENTATION DEEP-DIVE (order_items table)
----------------------------------------------------------

Objective: Find top order prices globally vs. a single day.
1) Unpruned Query:
    SELECT order_id, round(SUM(unit_price),2) as total_price FROM retailflow_raw.order_items GROUP BY order_id ORDER BY total_price DESC LIMIT 10;

    * Data Scanned: 6.36 MB
    * Run Time: 1.062 seconds
    ![alt text](<3) order_items_unpruned_query.png>)

2) Pruned Query:
    SELECT order_id, round(SUM(unit_price),2) as total_price FROM retailflow_raw.order_items where dt='2026-07-09' GROUP BY order_id ORDER BY total_price DESC LIMIT 10;

    * Data Scanned: 1.24 MB
    * Run Time: 589 ms
    ![alt text](<3) order_items_pruned_query.png>)

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

6. SUMMARY MATRIX & FINANCIAL ANALYSIS

    Test 1 (Global Select)
    * Unpruned: 3.16 MB
    * Pruned: 618.23 KB
    * Data Reduction: 80.44% Data Saved
    * Impact: Cut query latency in half immediately.

    Test 2 (Status Metrics)
    * Unpruned: 3.16 MB
    * Pruned: 2.55 MB
    * Data Reduction: 19.30% Data Saved
    * Impact: Lowered query planning stress on the engine.

    Test 3 (Top Item Revenue)
    * Unpruned: 6.36 MB
    * Pruned: 1.24 MB
    * Data Reduction: 80.50% Data Saved
    * Impact: Kept costs completely flat even as the dataset grows.

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

7. CONCLUSION:
Using Partition Projection completely cuts out data lake maintenance routines. Forcing queries to filter on the "dt" partition key drops data scan sizes by up to 80.50%. This guarantees low costs and fast speeds as the data lake grows over time.