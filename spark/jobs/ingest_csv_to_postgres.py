import argparse, os
from pyspark.sql import functions as F, types as T
from common_spark import get_spark

PG_URL = "jdbc:postgresql://{}:{}/{}".format(
    os.getenv("POSTGRES_HOST","postgres"), os.getenv("POSTGRES_PORT","5432"), os.getenv("POSTGRES_DB","retail")
)

pg_props = {
    "user": os.getenv("POSTGRES_USER","retail"),
    "password": os.getenv("POSTGRES_PASSWORD","retail"),
    "driver": "org.postgresql.Driver",
}

schema = T.StructType([
    T.StructField("InvoiceNo", T.StringType()),
    T.StructField("StockCode", T.StringType()),
    T.StructField("Description", T.StringType()),
    T.StructField("Quantity", T.IntegerType()),
    T.StructField("InvoiceDate", T.TimestampType()),
    T.StructField("UnitPrice", T.DoubleType()),
    T.StructField("CustomerID", T.IntegerType()),
    T.StructField("Country", T.StringType()),
])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", default="online_retail_ii/")
    args = parser.parse_args()

    spark = get_spark("retail_ingest")
    df = (
        spark.read.option("header", True).schema(schema)
        .csv(f"s3a://{os.environ['S3_BUCKET_RAW']}/{args.prefix}")
    )

    dq = df.select(
        F.count(F.when(F.col("InvoiceNo").isNull(), 1)).alias("null_invoice"),
        F.count(F.when(F.col("Quantity") < 0, 1)).alias("neg_qty"),
        F.count(F.when(F.col("UnitPrice") < 0, 1)).alias("neg_price"),
    ).collect()[0]
    assert dq["neg_qty"] == 0 and dq["neg_price"] == 0, f"DQ failed: {dq}"

    df = df.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate"))
    df = df.withColumn("ingested_at", F.current_timestamp())

    df.write.mode("overwrite").format("jdbc").option("url", PG_URL).options(**pg_props).option(
        "dbtable", "stage.online_retail_ii"
    ).save()
    spark.stop()
