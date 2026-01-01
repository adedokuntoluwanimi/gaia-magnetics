import uuid
from typing import Optional

from fastapi import (
    APIRouter,
    UploadFile,
    File,
    Form,
    HTTPException,
)

from app.core.job_runner import JobRunner
from app.schemas.job import JobCreateRequest, JobResponse


router = APIRouter()
runner = JobRunner()


@router.post("", response_model=JobResponse)
async def create_job(
    file: UploadFile = File(...),

    scenario: str = Form(...),
    x_column: str = Form(...),
    y_column: str = Form(...),
    value_column: str = Form(...),
    spacing: Optional[float] = Form(None),
):
    """
    Creates and executes a GAIA Magnetics job synchronously.
    """

    job_id = uuid.uuid4().hex

    try:
        request = JobCreateRequest(
            scenario=scenario,
            x_column=x_column,
            y_column=y_column,
            value_column=value_column,
            spacing=spacing,
        )

        await runner.prepare(job_id, file, request)
        runner.run(job_id)

        return JobResponse(job_id=job_id)

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e),
        )
