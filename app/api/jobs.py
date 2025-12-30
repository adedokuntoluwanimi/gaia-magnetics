from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from typing import Optional

from app.core.job_runner import JobRunner

router = APIRouter()


@router.post("")
def create_job(
    file: UploadFile = File(...),
    scenario: str = Form(...),
    x_column: str = Form(...),
    y_column: str = Form(...),
    tmi_column: str = Form(...),
    station_spacing: Optional[float] = Form(None),
):
    if scenario not in ("sparse", "explicit"):
        raise HTTPException(status_code=400, detail="Invalid scenario")

    if scenario == "sparse" and station_spacing is None:
        raise HTTPException(
            status_code=400,
            detail="station_spacing is required for sparse geometry",
        )

    runner = JobRunner()

    try:
        result = runner.run(
            upload_file=file,
            scenario=scenario,
            x_column=x_column,
            y_column=y_column,
            tmi_column=tmi_column,
            station_spacing=station_spacing,
        )
    except Exception as e:
        # Keep this blunt for now. We want to SEE failures.
        raise HTTPException(status_code=500, detail=str(e))

    return result


@router.get("/{job_id}/download")
def download_result(job_id: str):
    runner = JobRunner()

    try:
        return runner.get_final_csv(job_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Job not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
