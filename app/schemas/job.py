from pydantic import BaseModel, Field
from typing import Optional, Literal


class JobCreateRequest(BaseModel):
    scenario: Literal["explicit", "sparse"]

    x_column: str = Field(..., description="X coordinate column name")
    y_column: str = Field(..., description="Y coordinate column name")
    value_column: str = Field(..., description="Magnetic value column name")

    spacing: Optional[float] = Field(
        None,
        description="Output spacing for sparse geometry",
    )


class JobResponse(BaseModel):
    job_id: str
