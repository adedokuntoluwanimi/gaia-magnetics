from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse
import uuid
import tempfile
import os
from typing import Optional

from app.core.job_runner import JobRunner
from app.core.s3_io import S3IO

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("")
def create_job(
    file: UploadFile = File(...),
    x_col: str = Form(...),
    y_col: str = Form(...),
    tmi_col: str = Form(...),
    scenario: str = Form(...),
    station_spacing: Optional[float] = Form(None),

):
    job_id = f"gaia-{uuid.uuid4().hex[:12]}"

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(file.file.read())
        tmp_path = tmp.name

    try:
        runner = JobRunner()
        result = runner.run(
            job_id=job_id,
            scenario=scenario,
            csv_path=tmp_path,
            x_col=x_col,
            y_col=y_col,
            tmi_col=tmi_col,
            station_spacing=station_spacing,
        )
        return result
    finally:
        os.remove(tmp_path)


@router.get("/{job_id}/download")
def download_final_csv(job_id: str):
    """
    Download final merged CSV for a job.
    """

    s3 = S3IO()
    key = f"{s3.job_prefix(job_id)}final/final.csv"

    try:
        obj = s3.s3.get_object(Bucket=s3.bucket, Key=key)
    except Exception:
        raise HTTPException(status_code=404, detail="Final CSV not found")

    return StreamingResponse(
        obj["Body"],
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{job_id}_final.csv"'
        },
    )
