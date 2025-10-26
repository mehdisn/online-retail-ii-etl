import os

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "retail-raw")
S3_BUCKET_FEATURES = os.getenv("S3_BUCKET_FEATURES", "retail-features")

POSTGRES_CONN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "db": os.getenv("POSTGRES_DB", "retail"),
    "user": os.getenv("POSTGRES_USER", "retail"),
    "password": os.getenv("POSTGRES_PASSWORD", "retail"),
}
CLICKHOUSE_CONN = {
    "host": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
    "port": int(os.getenv("CLICKHOUSE_PORT", 9000)),
    "user": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
    "database": os.getenv("CLICKHOUSE_DB", "retail"),
}
