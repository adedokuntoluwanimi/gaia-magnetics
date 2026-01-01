from pydantic import BaseSettings


class Settings(BaseSettings):
    """
    Central configuration for GAIA Magnetics backend.
    Batch Transform–based execution.
    """

    # --------------------------------------------------
    # AWS / Infrastructure
    # --------------------------------------------------
    aws_region: str = "us-east-1"
    s3_bucket: str = "gaia-magnetics"

    # --------------------------------------------------
    # SageMaker (Batch Transform)
    # --------------------------------------------------
    sagemaker_model_name: str = "gaia-magnetics-model"

    class Config:
        env_prefix = "GAIA_"


settings = Settings()
