from pydantic import BaseModel
from typing import Optional
from app.core.models import JobStatus


class JobCreateRequest(BaseModel):
    scenario: str  # "sparse" or "explicit"
    x_column: str
    y_column: str
    tmi_column: str
    station_spacing: Optional[float] = None


class JobCreateResponse(BaseModel):
    job_id: str
    status: JobStatus
    train_count: int
    predict_count: int
