import os
from pydantic import BaseSettings


class Settings(BaseSettings):
    s3_bucket = "gaia-magnetics"
    aws_region = "us-east-1"
    sagemaker_endpoint_name = "gaia-magnetics-endpoint"


    class Config:
        env_prefix = "GAIA_"


settings = Settings()
