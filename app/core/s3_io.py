import os
import boto3

S3_BUCKET = os.getenv("GAIA_S3_BUCKET")

if not S3_BUCKET:
    raise RuntimeError("GAIA_S3_BUCKET environment variable not set")

s3 = boto3.client("s3")


def upload_raw_csv(job_id: str, content: bytes, filename: str):
    key = f"jobs/{job_id}/input/{filename}"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=content,
        ContentType="text/csv",
    )

    return key
