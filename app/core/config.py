import os
from pydantic import BaseSettings


class Settings(BaseSettings):
    s3_bucket = "gaia-magnetics"
    aws_region = "us-east-1"


    class Config:
        env_prefix = "GAIA_"


settings = Settings()
