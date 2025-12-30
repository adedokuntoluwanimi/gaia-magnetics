# app/api/jobs.py

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional
import io
from app.schemas.job import Scenario
import boto3

from app.core.job_runner import JobRunner


router = APIRouter(prefix="/jobs", tags=["jobs"])


# -------------------------------------------------
# Config (explicit, no magic)
# -------------------------------------------------

S3_BUCKET = "gaia"
AWS_REGION = "us-east-1"
SAGEMAKER_ENDPOINT = "gaia-train-on-the-fly"


def get_job_runner() -> JobRunner:
    return JobRunner(
        s3_bucket=S3_BUCKET,
        s3_region=AWS_REGION,
        sagemaker_endpoint=SAGEMAKER_ENDPOINT,
    )


# -------------------------------------------------
# POST /jobs
# -------------------------------------------------

@router.post("")
def create_job(
    file: UploadFile = File(...),
    scenario: Scenario = Form(...),
    x_column: str = Form(...),
    y_column: str = Form(...),
    tmi_column: str = Form(...),
    station_spacing: Optional[float] = Form(None),
):
    try:
        runner = get_job_runner()

        job_id = runner.run(
            csv_file=io.TextIOWrapper(file.file, encoding="utf-8"),
            scenario=scenario,
            x_column=x_column,
            y_column=y_column,
            tmi_column=tmi_column,
            station_spacing=station_spacing,
        )

        return {"job_id": job_id}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# -------------------------------------------------
# GET /jobs/{job_id}/download
# -------------------------------------------------

@router.get("/{job_id}/download")
def download_result(job_id: str):
    s3 = boto3.client("s3", region_name=AWS_REGION)

    key = f"{job_id}/final.csv"

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="Result not found")

    def stream():
        yield from obj["Body"].iter_lines()

    return StreamingResponse(
        stream(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={job_id}.csv"
        },
    )
