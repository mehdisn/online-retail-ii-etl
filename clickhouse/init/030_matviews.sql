CREATE MATERIALIZED VIEW IF NOT EXISTS retail.mv_daily_revenue
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date)
AS SELECT date, sum(amount) AS revenue FROM retail.fact_sales GROUP BY date;
