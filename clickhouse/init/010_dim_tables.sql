CREATE TABLE IF NOT EXISTS retail.dim_customer (
  CustomerID UInt32,
  customer_country String
) ENGINE = MergeTree ORDER BY (CustomerID);

CREATE TABLE IF NOT EXISTS retail.dim_product (
  StockCode String,
  product_desc String
) ENGINE = MergeTree ORDER BY (StockCode);

CREATE TABLE IF NOT EXISTS retail.dim_date (
  date Date
) ENGINE = MergeTree ORDER BY (date);
