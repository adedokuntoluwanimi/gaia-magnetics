import uuid
from fastapi import APIRouter, UploadFile, File, HTTPException

from app.core.s3_io import upload_raw_csv

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("")
async def create_job(csv_file: UploadFile = File(...)):
    if not csv_file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    job_id = f"gaia-{uuid.uuid4().hex}"

    content = await csv_file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty CSV file")

    upload_raw_csv(
        job_id=job_id,
        content=content,
        filename=csv_file.filename,
    )

    return {
        "job_id": job_id,
        "status": "created",
    }
