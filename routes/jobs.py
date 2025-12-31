import uuid
from fastapi import APIRouter, UploadFile, File, HTTPException

from app.core.s3_io import upload_raw_csv
from app.core.job_store import create_job_record
from app.core.job_store import get_job_record


router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("")
async def create_job(csv_file: UploadFile = File(...)):
    if not csv_file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    content = await csv_file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty CSV file")

    job_id = f"gaia-{uuid.uuid4().hex}"

    create_job_record(job_id)

    upload_raw_csv(
        job_id=job_id,
        content=content,
        filename=csv_file.filename,
    )

    return {
        "job_id": job_id,
        "status": "created",
    }



@router.get("/{job_id}/status")
def job_status(job_id: str):
    return get_job_record(job_id)
