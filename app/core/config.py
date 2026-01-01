print("CONFIG LOADED FROM:", __file__)
from pydantic import BaseSettings


class Settings(BaseSettings):
    aws_region: str = "us-east-1"
    s3_bucket: str = "gaia-magnetics"

    # REQUIRED for Batch Transform
    sagemaker_model_name: str = "gaia-magnetics-model"

    class Config:
        env_prefix = "GAIA_"


settings = Settings()

