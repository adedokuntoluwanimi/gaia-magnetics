import json
import boto3
from datetime import datetime
from app.core.config import settings
from app.core.config import settings
print("JOB_STORE_BUCKET =", settings.s3_bucket)


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
        Body=json.dumps(record).encode(),
        ContentType="application/json",
    )

    return record


def update_job_status(job_id: str, status: str):
    obj = s3.get_object(
        Bucket=settings.s3_bucket,
        Key=_job_key(job_id),
    )

    record = json.loads(obj["Body"].read())
    record["status"] = status
    record["updated_at"] = datetime.utcnow().isoformat()

    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=_job_key(job_id),
        Body=json.dumps(record).encode(),
        ContentType="application/json",
    )

    return record


def get_job_record(job_id: str):
    obj = s3.get_object(
        Bucket=settings.s3_bucket,
        Key=_job_key(job_id),
    )

    return json.loads(obj["Body"].read())
