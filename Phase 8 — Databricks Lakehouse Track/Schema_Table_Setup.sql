-- 1. Create and select the schema namespace
CREATE CATALOG IF NOT EXISTS main;
USE CATALOG main;

CREATE SCHEMA IF NOT EXISTS clickstream_schema;
USE SCHEMA clickstream_schema;

-- 2. Create the Bronze Table (Managed Delta Lake Table)
CREATE TABLE IF NOT EXISTS main.clickstream_schema.bronze_clickstream (
  session_id STRING,
  customer_id STRING,
  event_type STRING,
  event_timestamp STRING,
  page_url STRING,
  device_type STRING
) 
USING DELTA
PARTITIONED BY (dt STRING);


-- 3. Dashboard Analytics Queries---
-- Query 1: Total Event Volume KPI Tile
SELECT SUM(total_events) AS total_processed_events 
FROM main.clickstream_schema.gold_metrics;

-- Query 2: Event Type Distribution Bar Chart
SELECT event_type, total_events 
FROM main.clickstream_schema.gold_metrics;

-- Query 3: Unique Customer Volume Pie Chart
SELECT event_type, unique_customers 
FROM main.clickstream_schema.gold_metrics;