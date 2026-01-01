import uuid
from typing import Optional

from fastapi import (
    APIRouter,
    UploadFile,
    File,
    Form,
    HTTPException,
)

from app.core.job_store import get_job_record
from app.core.job_runner import JobRunner
from app.schemas.job import JobCreateRequest, Scenario


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

    # required only for sparse
    station_spacing: Optional[float] = Form(None),
):
    if not csv_file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    if scenario == Scenario.sparse and station_spacing is None:
        raise HTTPException(
            status_code=400,
            detail="station_spacing is required for sparse geometry",
        )

    job_id = f"gaia-{uuid.uuid4().hex}"

    request = JobCreateRequest(
        scenario=scenario,
        x_column=x_column,
        y_column=y_column,
        value_column=value_column,
        station_spacing=station_spacing,
    )

    runner = JobRunner(job_id)
    await runner.run(csv_file, request)

    return {
        "job_id": job_id,
        "status": "running",
    }


@router.get("/{job_id}/status")
def job_status(job_id: str):
    return get_job_record(job_id)
