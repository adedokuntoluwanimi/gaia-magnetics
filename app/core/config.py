from pydantic import BaseSettings


class Settings(BaseSettings):
    # AWS
    aws_region: str = "us-east-1"
    s3_bucket: str = "gaia-magnetics"

    # SageMaker
    sagemaker_model_name: str = "gaia-magnetics-model"

    class Config:
        env_prefix = "GAIA_"


settings = Settings()
