# app/schemas/job.py

from enum import Enum
from pydantic import BaseModel


# -------------------------------------------------
# Scenario enum (locked)
# -------------------------------------------------

class Scenario(str, Enum):
    sparse = "sparse"
    explicit = "explicit"


# -------------------------------------------------
# Job creation response
# -------------------------------------------------

class JobResponse(BaseModel):
    job_id: str
