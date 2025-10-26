import os
from common_spark import get_spark
from pyspark.sql import functions as F

spark = get_spark("retail_features")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", 8123))
CH_DB = os.getenv("CLICKHOUSE_DB", "retail")
CH_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}"

src = (
    spark.read.format("clickhouse")
    .option("url", CH_URL)
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", f"{CH_DB}.fact_sales") # Read from fact_sales
    .load()
)

rfm = (src
   .groupBy("CustomerID")
   .agg(
        F.max("InvoiceDate").alias("last_purchase"), 
        F.countDistinct("InvoiceNo").alias("frequency"),
        F.sum("amount").alias("monetary"), # Use pre-calculated amount
   )
   .withColumn("recency_days", F.datediff(F.current_timestamp(), F.col("last_purchase")))
   .select("CustomerID","recency_days","frequency","monetary")
)

rfm.write.mode("overwrite").parquet(f"s3a://{os.environ['S3_BUCKET_FEATURES']}/rfm/")
