import json
from datetime import datetime
import boto3

from app.core.config import settings


s3 = boto3.client(
    "s3",
    region_name=settings.aws_region,
)


def _job_key(job_id: str) -> str:
    return f"jobs/{job_id}/metadata/job.json"


def create_job_record(job_id: str):
    record = {
        "job_id": job_id,
        "status": "created",
        "created_at": datetime.utcnow().isoformat(),
    }

    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=_job_key(job_id),
        Body=json.dumps(record).encode("utf-8"),
        ContentType="application/json",
    )

    return record


def update_job_status(job_id: str, status: str):
    record = get_job_record(job_id)

    record["status"] = status
    record["updated_at"] = datetime.utcnow().isoformat()

    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=_job_key(job_id),
        Body=json.dumps(record).encode("utf-8"),
        ContentType="application/json",
    )

    return record


def get_job_record(job_id: str):
    try:
        obj = s3.get_object(
            Bucket=settings.s3_bucket,
            Key=_job_key(job_id),
        )
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        # Defensive: job was requested before record creation
        return {
            "job_id": job_id,
            "status": "unknown",
        }
