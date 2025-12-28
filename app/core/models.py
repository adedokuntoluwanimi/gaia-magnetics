from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


class JobStatus(str, Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    INFERENCING = "INFERENCING"
    MERGING = "MERGING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class Job:
    job_id: str
    status: JobStatus
    scenario: str
    station_spacing: Optional[float]
    input_csv_s3_path: Optional[str] = None
    train_csv_s3_path: Optional[str] = None
    predict_csv_s3_path: Optional[str] = None
    prediction_csv_s3_path: Optional[str] = None
    final_csv_s3_path: Optional[str] = None
    created_at: datetime = datetime.utcnow()
    updated_at: datetime = datetime.utcnow()
