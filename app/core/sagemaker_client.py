# app/core/sagemaker_client.py

import json
import boto3
from app.core.config import settings
from app.core.s3_io import upload_raw_csv


class SageMakerClient:
    def __init__(self):
        self.client = boto3.client(
            "sagemaker-runtime",
            region_name=settings.aws_region,
        )
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

        # 1. Upload request JSON to S3
        request_key = "inference/request.json"

        upload_raw_csv(
            job_id=job_id,
            content=json.dumps(payload).encode(),
            filename=request_key,
        )

        # 2. Invoke async endpoint with S3 URI
        input_location = (
            f"s3://{settings.s3_bucket}/jobs/{job_id}/{request_key}"
        )

        response = self.client.invoke_endpoint_async(
            EndpointName=self.endpoint_name,
            ContentType="application/json",
            InputLocation=input_location,
        )

        return response
