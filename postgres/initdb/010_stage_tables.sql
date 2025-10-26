CREATE TABLE IF NOT EXISTS stage.online_retail_ii (
  InvoiceNo    text,
  StockCode    text,
  Description  text,
  Quantity     int,
  InvoiceDate  timestamp,
  UnitPrice    double precision,
  CustomerID   int,
  Country      text,
  ingested_at  timestamp default now()
);
CREATE INDEX IF NOT EXISTS idx_stage_invoice_date ON stage.online_retail_ii(InvoiceDate);
