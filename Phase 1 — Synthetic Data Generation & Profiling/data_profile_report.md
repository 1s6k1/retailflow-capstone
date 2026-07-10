FILE: customers.csv
Shape: 5150 rows, 4 columns
Total Duplicates: 150
       Column Dtype Null %            Range/Cardinality
  customer_id   str  0.00% [5000 distinct clean values]
customer_name   str  0.00% [4823 distinct clean values]
        email   str  7.90% [4552 distinct clean values]
      country   str  0.58%  [194 distinct clean values]
------------------------------------------------------------
FILE: products.csv
Shape: 800 rows, 5 columns
Total Duplicates: 0
      Column   Dtype Null %           Range/Cardinality
  product_id     str  0.00% [800 distinct clean values]
product_name     str  0.00% [800 distinct clean values]
    category     str  0.00%   [5 distinct clean values]
  unit_price float64  0.00%            [5.08 to 299.94]
 active_flag    bool  0.00%             [False to True]
------------------------------------------------------------
FILE: orders_day1.json
Shape: 20000 rows, 5 columns
Total Duplicates: 0
      Column Dtype Null %             Range/Cardinality
    order_id   str  0.00% [20000 distinct clean values]
 customer_id   str  0.00%  [4918 distinct clean values]
    order_ts   str  0.00% [17887 distinct clean values]
store_region   str  0.00%     [5 distinct clean values]
      status   str  0.00%     [3 distinct clean values]
------------------------------------------------------------
FILE: orders_day2.json
Shape: 4000 rows, 6 columns
Total Duplicates: 0
       Column Dtype Null %            Range/Cardinality
     order_id   str  0.00% [4000 distinct clean values]
  customer_id   str  0.00% [2745 distinct clean values]
     order_ts   str  0.00% [3902 distinct clean values]
 store_region   str  0.00%    [5 distinct clean values]
       status   str  0.00%    [3 distinct clean values]
discount_code   str 39.98%    [3 distinct clean values]
------------------------------------------------------------
FILE: order_items_day1.json
Shape: 49995 rows, 5 columns
Total Duplicates: 0
    Column   Dtype Null %             Range/Cardinality
  order_id     str  0.00% [20000 distinct clean values]
product_id     str  0.00%   [800 distinct clean values]
  quantity   int64  0.00%                      [1 to 4]
unit_price float64  0.00%              [5.08 to 299.94]
line_total float64  0.00%             [5.08 to 1199.76]
------------------------------------------------------------
FILE: order_items_day2.json
Shape: 9901 rows, 6 columns
Total Duplicates: 0
       Column   Dtype Null %            Range/Cardinality
     order_id     str  0.00% [4000 distinct clean values]
   product_id     str  0.00%  [800 distinct clean values]
     quantity   int64  0.00%                     [1 to 4]
   unit_price float64  0.00%             [5.08 to 299.94]
   line_total float64  0.00%            [5.08 to 1199.76]
discount_code     str 39.95%    [3 distinct clean values]
------------------------------------------------------------
FILE: clickstream_day1.json
Shape: 15000 rows, 6 columns
Total Duplicates: 0
         Column Dtype Null %             Range/Cardinality
     session_id   str  0.00% [15000 distinct clean values]
    customer_id   str 30.19%  [4383 distinct clean values]
event_timestamp   str  0.00% [13769 distinct clean values]
     event_type   str  0.00%     [6 distinct clean values]
       page_url   str  0.00%  [1781 distinct clean values]
    device_type   str  0.00%     [3 distinct clean values]
------------------------------------------------------------
FILE: clickstream_day2.json
Shape: 15000 rows, 6 columns
Total Duplicates: 0
         Column Dtype Null %             Range/Cardinality
     session_id   str  0.00% [15000 distinct clean values]
    customer_id   str 30.40%  [4378 distinct clean values]
event_timestamp   str  0.00% [13770 distinct clean values]
     event_type   str  0.00%     [6 distinct clean values]
       page_url   str  0.00%  [1794 distinct clean values]
    device_type   str  0.00%     [3 distinct clean values]
------------------------------------------------------------