from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import boto3, os

class MinioKeySensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, bucket, prefix, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix

    def poke(self, context):
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT_URL"],
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        )
        resp = s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
        return resp.get("KeyCount", 0) > 0
