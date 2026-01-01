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
