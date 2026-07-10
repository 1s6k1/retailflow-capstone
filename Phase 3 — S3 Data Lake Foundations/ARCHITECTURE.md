AWS S3 Data Lake Architecture:
A single Amazon S3 bucket with three top-level folders (zones) to organize data based on its processing stage.

1. Bronze Zone (raw/)
* Stores original data exactly as received from the source.
* No changes or transformations are made.
* Used for backup and reprocessing if needed.

2. Silver Zone (curated/)
* Stores cleaned and validated data.
* Removes duplicate or invalid records.
* Data is ready for further processing.

3. Gold Zone (consumption/)
* Stores final business-ready data.
* Used for reporting, dashboards, and analytics.

---------------------------------------------------------------------------

Bucket Structure:
retailflow-bucket-2026/
|
├── raw/
│   ├── clickstream/
│   │   ├── dt=2026-07-08/
│   │   └── dt=2026-07-09/
│   ├── customers/
│   ├── order_items/
│   │   ├── dt=2026-07-08/
│   │   └── dt=2026-07-09/
│   ├── orders/
│   │   ├── dt=2026-07-08/
│   │   └── dt=2026-07-09/
│   └── products/
│
├── curated/
│   ├── clickstream/
│   │   ├── dt=2026-07-08/
│   │   └── dt=2026-07-09/
│   ├── customers/
│   ├── order_items/
│   │   ├── dt=2026-07-08/
│   │   └── dt=2026-07-09/
│   ├── orders/
│   │   ├── dt=2026-07-08/
│   │   └── dt=2026-07-09/
│   └── products/
│
└── consumption/
    ├── sales_summary/
    └── customer_summary/

---------------------------------------------------------------------------

Naming Convention:
Bronze layer   : raw/
Silver layer   : curated/
Gold layer     : consumption/
Dataset names  : orders, customers, order_items, products, clickstream
Date partition : dt=YYYY-MM-DD
dt=2026-07-08  : Stores Day 1 dataset
dt=2026-07-09  : Stores Day 2 dataset

---------------------------------------------------------------------------

Partitioning Convention:
The raw and curated folder is partitioned by:
* Dataset name
* Ingestion date

Format: raw/<dataset-name>/dt=YYYY-MM-DD/, curated/<dataset-name>/dt=YYYY-MM-DD/

---------------------------------------------------------------------------

Summary:
The S3 bucket is organized into three zones:
* raw/ for original data
* curated/ for cleaned data
* consumption/ for analytics-ready data
