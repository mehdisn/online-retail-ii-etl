import os
from pyspark.sql import SparkSession

def get_spark(app_name="retail_spark"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", os.getenv("TZ", "UTC"))
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT_URL"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", True)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
