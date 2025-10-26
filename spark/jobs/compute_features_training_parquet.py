import os
from common_spark import get_spark
from pyspark.sql import functions as F

spark = get_spark("retail_features")
pg = ("jdbc:postgresql://{}:{}/{}".format(os.getenv("POSTGRES_HOST","postgres"), os.getenv("POSTGRES_PORT","5432"), os.getenv("POSTGRES_DB","retail")))
props = {"user": os.getenv("POSTGRES_USER","retail"), "password": os.getenv("POSTGRES_PASSWORD","retail"), "driver": "org.postgresql.Driver"}

src = spark.read.format("jdbc").option("url", pg).options(**props).option("dbtable", "stage.online_retail_ii").load()

rfm = (src
   .groupBy("CustomerID")
   .agg(
        F.max("InvoiceDate").alias("last_purchase"),
        F.countDistinct("InvoiceNo").alias("frequency"),
        F.sum(F.col("Quantity")*F.col("UnitPrice")).alias("monetary"),
   )
   .withColumn("recency_days", F.datediff(F.current_timestamp(), F.col("last_purchase")))
   .select("CustomerID","recency_days","frequency","monetary")
)

rfm.write.mode("overwrite").parquet(f"s3a://{os.environ['S3_BUCKET_FEATURES']}/rfm/")
