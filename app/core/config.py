import os
from pydantic import BaseSettings


class Settings(BaseSettings):
    S3_BUCKET = "gaia-ml-dev"
    AWS_REGION = "us-east-1"


    class Config:
        env_prefix = "GAIA_"


settings = Settings()
