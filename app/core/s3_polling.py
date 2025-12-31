import time
import boto3
from app.core.config import settings


s3 = boto3.client("s3", region_name=settings.aws_region)


def wait_for_object(bucket: str, key: str, timeout: int = 900, interval: int = 5):
    """
    Polls S3 until an object exists or timeout is reached.
    """
    start = time.time()

    while True:
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except s3.exceptions.ClientError:
            if time.time() - start > timeout:
                raise TimeoutError("Timed out waiting for predictions.csv")
            time.sleep(interval)
