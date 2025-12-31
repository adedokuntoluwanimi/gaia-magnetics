import uuid
from fastapi import (
    APIRouter,
    UploadFile,
    File,
    Form,
    HTTPException,
)

from app.core.job_store import create_job_record, get_job_record
from app.core.job_runner import JobRunner
from app.schemas.job import Scenario
from typing import Optional



router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("")
async def create_job(
    csv_file: UploadFile = File(...),

    # scenario selection
    scenario: Scenario = Form(...),

    # column mappings
    x_column: str = Form(...),
    y_column: str = Form(...),
    value_column: str = Form(...),

    # optional for sparse geometry
    station_spacing: Optional[float] = Form(None),

):
    if not csv_file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    content = await csv_file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty CSV file")

    if scenario == Scenario.sparse and station_spacing is None:

        raise HTTPException(
            status_code=400,
            detail="station_spacing is required for sparse geometry",
        )

    job_id = f"gaia-{uuid.uuid4().hex}"

    # create job metadata first
    create_job_record(job_id)

    # hand off to pipeline
    runner = JobRunner()
    runner.run(
        job_id=job_id,
        scenario=scenario.value,
        csv_bytes=content,
        x_col=x_column,
        y_col=y_column,
        value_col=value_column,
        station_spacing=station_spacing,
    )

    return {
        "job_id": job_id,
        "status": "running",
    }


@router.get("/{job_id}/status")
def job_status(job_id: str):
    return get_job_record(job_id)
