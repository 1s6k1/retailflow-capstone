COPY dim_customer
FROM 's3://retailflow-bucket-2026/curated/customers/part-00000-5a3c6736-2bad-48aa-ad09-1a90300caee4-c000.snappy.parquet'
IAM_ROLE 'arn:aws:iam::478186673765:role/Redshift_S3_Role'
FORMAT AS PARQUET;


COPY dim_product
FROM 's3://retailflow-bucket-2026/curated/products/part-00000-b073352e-6ca1-4a71-81aa-ea9c26b72f0e-c000.snappy.parquet'
IAM_ROLE 'arn:aws:iam::478186673765:role/Redshift_S3_Role'
FORMAT AS PARQUET;


---load data to DIM_date table
SELECT 
    datum AS full_date,
    TO_CHAR(datum, 'Day') AS day_of_week,
    TO_CHAR(datum, 'Month') AS month,
    EXTRACT(year FROM datum) AS year
FROM (
    SELECT '2026-01-01'::DATE + idx AS datum
    FROM (
        SELECT ROW_NUMBER() OVER () - 1 AS idx
        FROM pg_catalog.pg_type a, pg_catalog.pg_type b
    )
    WHERE idx <= ('2026-12-31'::DATE - '2026-01-01'::DATE)
);


---insert data from spectrum_schema.fact_order_items to redshift fact_order_items table
INSERT INTO fact_order_items (order_id, product_id, quantity, unit_price, line_total, discount_code, dt)
SELECT order_id, product_id, quantity, unit_price, line_total, discount_code, dt
FROM spectrum_schema.fact_order_items;
