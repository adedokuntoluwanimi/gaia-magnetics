# app/core/sagemaker_client.py

import json
import boto3
from app.core.config import settings


class SageMakerClient:
    def __init__(self):
        self.client = boto3.client(
            "sagemaker-runtime",
            region_name=settings.aws_region,
        )

        # name of your deployed async endpoint
        self.endpoint_name = settings.sagemaker_endpoint_name

    def invoke_async(
        self,
        job_id: str,
        train_s3: str,
        predict_s3: str,
        output_s3: str,
    ):
        payload = {
            "job_id": job_id,
            "train_s3": train_s3,
            "predict_s3": predict_s3,
            "output_s3": output_s3,
        }

        response = self.client.invoke_endpoint_async(
            EndpointName=self.endpoint_name,
            ContentType="application/json",
            InputLocation=json.dumps(payload),
        )

        return response
