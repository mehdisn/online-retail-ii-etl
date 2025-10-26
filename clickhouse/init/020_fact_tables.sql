CREATE TABLE IF NOT EXISTS retail.fact_sales (
  InvoiceNo String,
  CustomerID UInt32,
  StockCode String,
  date Date,
  Quantity Int32,
  UnitPrice Float64,
  amount Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (CustomerID, StockCode, date, InvoiceNo);
