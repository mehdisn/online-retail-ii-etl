import argparse, os
from pyspark.sql import functions as F
from common_spark import get_spark

CH_HOST = os.getenv("CLICKHOUSE_HOST","clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", 8123)) 
CH_DB = os.getenv("CLICKHOUSE_DB","retail")
CH_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}"

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)
    p.add_argument("--mode", default="incremental", choices=["incremental","backfill"])
    args = p.parse_args()

    spark = get_spark("retail_transform")
    pg = ("jdbc:postgresql://{}:{}/{}".format(os.getenv("POSTGRES_HOST","postgres"), os.getenv("POSTGRES_PORT","5432"), os.getenv("POSTGRES_DB","retail")))
    props = {"user": os.getenv("POSTGRES_USER","retail"), "password": os.getenv("POSTGRES_PASSWORD","retail"), "driver": "org.postgresql.Driver"}

    src = spark.read.format("jdbc").option("url", pg).options(**props).option("dbtable", "stage.online_retail_ii").load()

    if args.start and args.end and args.mode == 'backfill':
        # Backfill DAG filters on InvoiceDate
        print(f"Running backfill for InvoiceDate between {args.start} and {args.end}")
        src = src.filter(
            (F.col("InvoiceDate") >= F.to_timestamp(F.lit(args.start))) &
            (F.col("InvoiceDate") <= F.to_timestamp(F.lit(args.end)))
        )
    elif args.start and args.end and args.mode == 'incremental':
        # Daily DAG filters on ingested_at
        print(f"Running incremental load for ingested_at between {args.start} and {args.end}")
        src = src.filter(
            (F.col("ingested_at") >= F.to_timestamp(F.lit(args.start))) &
            (F.col("ingested_at") < F.to_timestamp(F.lit(args.end)))
        )
    else:
        # Default behavior if no args (optional)
        print("Warning: Running transform on entire source table.")
    dim_customer = src.select("CustomerID", "Country").where("CustomerID IS NOT NULL").dropDuplicates(["CustomerID"]).withColumnRenamed("Country","customer_country")
    dim_product = src.select("StockCode","Description").dropDuplicates(["StockCode"]).withColumnRenamed("Description","product_desc")
    
    fact_sales = (
        src.withColumn("date", F.to_date("InvoiceDate"))
           .withColumn("amount", F.col("Quantity")*F.col("UnitPrice"))
           # Add InvoiceDate, which is needed for ML job (see Step 3)
           .select("InvoiceNo","CustomerID","StockCode", "InvoiceDate", "date","Quantity","UnitPrice","amount") 
           .where("Quantity >= 0 and UnitPrice >= 0")
    )

    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, database=CH_DB)

    def write_to_clickhouse(df, table_name):
        df.write.format("clickhouse") \
          .option("url", CH_URL) \
          .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
          .option("dbtable", f"{CH_DB}.{table_name}") \
          .mode("append") \
          .save()

    write_to_clickhouse(dim_customer, "dim_customer")
    write_to_clickhouse(dim_product, "dim_product")
    write_to_clickhouse(fact_sales, "fact_sales")
    
    spark.stop()
