from fastapi import (
    APIRouter,
    UploadFile,
    File,
    Form,
    HTTPException,
)
from uuid import uuid4

from app.schemas.job import (
    JobCreateRequest,
    JobCreateResponse,
    JobStatusResponse,
    JobStatus,
    Scenario,
)
from app.core.job_runner import JobRunner


router = APIRouter(prefix="/jobs", tags=["jobs"])


# ============================================================
# POST /jobs
# Create a new processing job
# ============================================================

@router.post("", response_model=JobCreateResponse)
async def create_job(
    csv_file: UploadFile = File(...),

    scenario: Scenario = Form(...),
    x_column: str = Form(...),
    y_column: str = Form(...),
    value_column: str = Form(...),
    station_spacing: float | None = Form(None),
):
    """
    Creates a new GAIA processing job.

    - Accepts CSV upload via multipart/form-data
    - Validates scenario rules
    - Delegates execution to JobRunner
    """

    # ----------------------------
    # Build and validate request
    # ----------------------------
    try:
        job_request = JobCreateRequest(
            scenario=scenario,
            x_column=x_column,
            y_column=y_column,
            value_column=value_column,
            station_spacing=station_spacing,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # ----------------------------
    # Create job
    # ----------------------------
    job_id = f"job-{uuid4().hex[:12]}"

    runner = JobRunner(job_id=job_id)

    # ----------------------------
    # Kick off processing
    # ----------------------------
    try:
        await runner.run(
            csv_file=csv_file,
            request=job_request,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Job failed to start: {e}",
        )

    return JobCreateResponse(
        job_id=job_id,
        status=JobStatus.created,
    )


# ============================================================
# GET /jobs/{job_id}/status
# Check job status
# ============================================================

@router.get("/{job_id}/status", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Returns current job status.
    """

    runner = JobRunner(job_id=job_id)

    status, message = runner.get_status()

    return JobStatusResponse(
        job_id=job_id,
        status=status,
        message=message,
    )


# ============================================================
# GET /jobs/{job_id}/result.csv
# Download final merged CSV
# ============================================================

@router.get("/{job_id}/result.csv")
async def download_result_csv(job_id: str):
    """
    Returns the final merged CSV file.
    """

    runner = JobRunner(job_id=job_id)

    csv_path = runner.get_result_csv_path()

    if not csv_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Result not found or job not completed",
        )

    return {
        "file_path": str(csv_path)
    }


# ============================================================
# GET /jobs/{job_id}/result.json
# Fetch data for plotting
# ============================================================

@router.get("/{job_id}/result.json")
async def get_result_json(job_id: str):
    """
    Returns final merged data as JSON for plotting.
    """

    runner = JobRunner(job_id=job_id)

    if not runner.is_completed():
        raise HTTPException(
            status_code=400,
            detail="Job not completed yet",
        )

    return runner.load_result_json()
