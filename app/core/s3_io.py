import boto3
from app.core.config import settings

s3 = boto3.client(
    "s3",
    region_name=settings.aws_region,
)


def upload_raw_csv(job_id: str, content: bytes, filename: str) -> str:
    key = f"jobs/{job_id}/input/{filename}"

    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=content,
        ContentType="text/csv",
    )

    return key
def download_csv(job_id: str, filename: str) -> bytes:
    key = f"jobs/{job_id}/{filename}"
    response = s3.get_object(Bucket=settings.s3_bucket, Key=key)
    return response["Body"].read()


def object_exists(job_id: str, filename: str) -> bool:
    try:
        s3.head_object(
            Bucket=settings.s3_bucket,
            Key=f"jobs/{job_id}/{filename}",
        )
        return True
    except Exception:
        return False
