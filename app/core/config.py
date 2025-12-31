import os
from pydantic import BaseSettings


class Settings(BaseSettings):
    aws_region: str = "us-east-1"
    s3_bucket: str

    class Config:
        env_prefix = "GAIA_"


settings = Settings()
