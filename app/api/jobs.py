import uuid
import tempfile
from fastapi import APIRouter, UploadFile, File, Form, HTTPException

from app.schemas.job import JobCreateResponse
from app.core.models import JobStatus
from app.core.job_runner import JobRunner
from typing import Optional

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("", response_model=JobCreateResponse)
def create_job(
    file: UploadFile = File(...),
    scenario: str = Form(...),
    x_column: str = Form(...),
    y_column: str = Form(...),
    tmi_column: str = Form(...),
    station_spacing: Optional[float] = Form(None),

):
    # -----------------------------
    # Validate scenario rules
    # -----------------------------
    if scenario == "sparse" and station_spacing is None:
        raise HTTPException(
            status_code=400,
            detail="station_spacing is required for sparse geometry",
        )

    if scenario == "explicit" and station_spacing is not None:
        raise HTTPException(
            status_code=400,
            detail="station_spacing must be omitted for explicit geometry",
        )

    job_id = f"gaia-{uuid.uuid4().hex[:12]}"

    # -----------------------------
    # Save uploaded CSV temporarily
    # -----------------------------
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(file.file.read())
        tmp_path = tmp.name

    # -----------------------------
    # Run job orchestration
    # -----------------------------
    runner = JobRunner()
    result = runner.run(
        job_id=job_id,
        scenario=scenario,
        csv_path=tmp_path,
        x_col=x_column,
        y_col=y_column,
        tmi_col=tmi_column,
        station_spacing=station_spacing,
    )

    return JobCreateResponse(
        job_id=job_id,
        status=JobStatus.SUBMITTED,
        train_count=result["train_count"],
        predict_count=result["predict_count"],
    )
