-- Dimension: Customer (Uses the natural customer_id text field directly)
CREATE TABLE dim_customer (
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    country VARCHAR(50),
    PRIMARY KEY (customer_id)
) 
DISTSTYLE ALL SORTKEY (customer_id);

-- Dimension: Product (Uses the natural product_id text field directly)
CREATE TABLE dim_product (
    product_id VARCHAR(100) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    unit_price DOUBLE PRECISION,
    active_flag VARCHAR(50),     
    PRIMARY KEY (product_id)
) 
DISTSTYLE ALL 
SORTKEY (product_id);

-- Dimension: Date (Uses a standard DATE data type as the primary key)
CREATE TABLE dim_date (
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    month VARCHAR(10),
    year INT,
    PRIMARY KEY (full_date)
) 
DISTSTYLE ALL SORTKEY (full_date);

-- Fact: Order Items (Foreign keys now map directly to the text/date fields)
CREATE TABLE fact_order_items (
    order_id VARCHAR(100) NOT NULL,
    product_id VARCHAR(100) REFERENCES dim_product(product_id),
    quantity BIGINT,
    unit_price DOUBLE PRECISION,
    line_total DOUBLE PRECISION,
    discount_code VARCHAR(100),
    dt DATE REFERENCES dim_date(full_date), -- Stored directly as a standard DATE column
    PRIMARY KEY (order_id, product_id)
) 
DISTSTYLE KEY DISTKEY (product_id) 
SORTKEY (dt);

---external table creation to store 
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_schema
FROM DATA CATALOG
DATABASE 'retailflow_raw' 
IAM_ROLE 'arn:aws:iam::478186673765:role/Redshift_S3_Role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;


CREATE EXTERNAL TABLE spectrum_schema.fact_order_items (
    order_id VARCHAR(100),
    product_id VARCHAR(100),
    quantity BIGINT,
    unit_price DOUBLE PRECISION,
    line_total DOUBLE PRECISION,
    discount_code VARCHAR(100)
)
PARTITIONED BY (dt DATE)
STORED AS PARQUET
LOCATION 's3://retailflow-bucket-2026/curated/order_items/';

--Register the partition for Day 1 (July 5th)
ALTER TABLE spectrum_schema.fact_order_items 
ADD PARTITION (dt='2026-07-05') 
LOCATION 's3://retailflow-bucket-2026/curated/order_items/order_date=2026-07-05/';

--Register the partition for Day 2 (July 6th)
ALTER TABLE spectrum_schema.fact_order_items 
ADD PARTITION (dt='2026-07-06') 
LOCATION 's3://retailflow-bucket-2026/curated/order_items/order_date=2026-07-06/';


--MATERIALIZED view for daily category revenue
CREATE MATERIALIZED VIEW mv_daily_category_revenue
AUTO REFRESH YES
AS
SELECT 
    o.dt,
    p.category,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.quantity) AS total_units_sold,
    CAST(SUM(o.line_total) AS NUMERIC(18,2)) AS daily_revenue
FROM fact_order_items o
JOIN dim_product p ON o.product_id = p.product_id
GROUP BY o.dt, p.category;