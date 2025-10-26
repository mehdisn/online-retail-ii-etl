import argparse, os
from pyspark.sql import functions as F
from common_spark import get_spark
import clickhouse_connect

CH_HOST = os.getenv("CLICKHOUSE_HOST","clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT",9000))
CH_DB = os.getenv("CLICKHOUSE_DB","retail")

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

    if args.start and args.end:
        src = src.filter((F.col("InvoiceDate") >= F.to_timestamp(F.lit(args.start))) & (F.col("InvoiceDate") <= F.to_timestamp(F.lit(args.end))))

    dim_customer = src.select("CustomerID", "Country").where("CustomerID IS NOT NULL").dropDuplicates(["CustomerID"]).withColumnRenamed("Country","customer_country")
    dim_product = src.select("StockCode","Description").dropDuplicates(["StockCode"]).withColumnRenamed("Description","product_desc")
    dim_date = src.select(F.col("InvoiceDate").alias("ts")).withColumn("date", F.to_date("ts")).select("date").dropDuplicates()

    fact_sales = (
        src.withColumn("date", F.to_date("InvoiceDate"))
           .withColumn("amount", F.col("Quantity")*F.col("UnitPrice"))
           .select("InvoiceNo","CustomerID","StockCode","date","Quantity","UnitPrice","amount")
           .where("Quantity >= 0 and UnitPrice >= 0")
    )

    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, database=CH_DB)

    def to_ch(df, table, fields):
        rows = [tuple(x[c] for c in fields) for x in df.toPandas().to_dict("records")]
        client.insert(table, rows, column_names=fields)

    to_ch(dim_customer, "dim_customer", ["CustomerID","customer_country"])
    to_ch(dim_product, "dim_product", ["StockCode","product_desc"])
    to_ch(dim_date, "dim_date", ["date"])
    to_ch(fact_sales, "fact_sales", ["InvoiceNo","CustomerID","StockCode","date","Quantity","UnitPrice","amount"])

    spark.stop()
