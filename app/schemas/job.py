from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, root_validator


# ============================================================
# Scenario definition
# ============================================================

class Scenario(str, Enum):
    sparse = "sparse"
    explicit = "explicit"


# ============================================================
# Job creation request schema
# ============================================================

class JobCreateRequest(BaseModel):
    """
    Schema representing a job creation request.

    This schema is used AFTER multipart parsing.
    The CSV file itself is handled separately by FastAPI.
    """

    scenario: Scenario = Field(
        ...,
        description="Geometry scenario: sparse or explicit"
    )

    x_column: str = Field(
        ...,
        description="Column name for X coordinate (e.g. longitude or easting)"
    )

    y_column: str = Field(
        ...,
        description="Column name for Y coordinate (e.g. latitude or northing)"
    )

    value_column: str = Field(
        ...,
        description="Column name for magnetic value"
    )

    station_spacing: Optional[float] = Field(
        None,
        gt=0,
        description="Desired output station spacing (required for sparse)"
    )

    @root_validator
    def validate_scenario_rules(cls, values):
        scenario = values.get("scenario")
        spacing = values.get("station_spacing")

        if scenario == Scenario.sparse:
            if spacing is None:
                raise ValueError(
                    "station_spacing is required when scenario is 'sparse'"
                )

        if scenario == Scenario.explicit:
            if spacing is not None:
                raise ValueError(
                    "station_spacing must not be provided when scenario is 'explicit'"
                )

        return values


# ============================================================
# Job status model
# ============================================================

class JobStatus(str, Enum):
    created = "created"
    running = "running"
    completed = "completed"
    failed = "failed"


# ============================================================
# Job creation response
# ============================================================

class JobCreateResponse(BaseModel):
    job_id: str
    status: JobStatus


# ============================================================
# Job status response
# ============================================================

class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    message: Optional[str] = None


# ============================================================
# Final result metadata (used by frontend)
# ============================================================

class JobResultMetadata(BaseModel):
    """
    Metadata describing the final merged result.
    The actual CSV/JSON payload is fetched separately.
    """

    job_id: str
    total_points: int
    measured_points: int
    predicted_points: int
